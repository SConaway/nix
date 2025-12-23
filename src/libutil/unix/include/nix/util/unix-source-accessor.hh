#pragma once
///@file

#include <cassert>
#include <mutex>

#include "nix/util/signals.hh"
#include "nix/util/source-accessor.hh"
#include "nix/util/file-descriptor.hh"

namespace nix {
namespace unix {

/* The accessors for file/directory access are different, because we want them
   all to work with file descriptors. Technically that could be done on Linux using
   O_PATH descriptors, but that wouldn't work on Darwin. */

class UnixSourceAccessorBase : public SourceAccessor
{
protected:
    bool trackLastModified;
    /**
     * The most recent mtime seen by fstat(). This is a hack to
     * support dumpPathAndGetMtime(). Should remove this eventually.
     */
    std::time_t mtime = 0;

    UnixSourceAccessorBase(bool trackLastModified)
        : trackLastModified(trackLastModified)
    {
    }

    void updateMtime(std::time_t newMtime)
    {
        /* The contract is that trackLastModified implies that the caller uses the accessor
           from a single thread. Thus this is not a CAS loop. */
        if (trackLastModified)
            mtime = std::max(mtime, newMtime);
    }

public:
    std::optional<std::time_t> getLastModified() override
    {
        return trackLastModified ? std::optional{mtime} : std::nullopt;
    }
};

class UnixFileSourceAccessor : public UnixSourceAccessorBase
{
    AutoCloseFD fd;
    CanonPath rootPath;
    mutable std::once_flag statFlag;
    mutable struct ::stat cachedStat;

public:

    UnixFileSourceAccessor(AutoCloseFD fd_, CanonPath rootPath_, bool trackLastModified, struct ::stat * st = nullptr)
        : UnixSourceAccessorBase(trackLastModified)
        , fd(std::move(fd_))
        , rootPath(std::move(rootPath_))
    {
        displayPrefix = rootPath.abs();
        if (st) {
            std::call_once(statFlag, [this, st] {
                cachedStat = *st;
                updateMtime(cachedStat.st_mtime);
            });
        }
    }

    std::string showPath(const CanonPath & path) override
    {
        if (path.isRoot())
            return displayPrefix;
        return SourceAccessor::showPath(path);
    }

    DirEntries readDirectory(const CanonPath & path) override
    {
        throw NotADirectory("'%s' is not a directory", showPath(path));
    }

    std::string readLink(const CanonPath & path) override
    {
        throw NotASymlink("file '%s' is not a symlink", showPath(path));
    }

    bool pathExists(const CanonPath & path) override
    {
        return path.isRoot(); /* We know that we are accessing a regular file and not a directory. */
    }

    std::optional<std::filesystem::path> getPhysicalPath(const CanonPath & path) override
    {
        if (path.isRoot())
            return std::filesystem::path(rootPath.abs());
        /* Slightly different than what PosixSourceAccessor used to do, but we know that this is not a directory. */
        return std::nullopt;
    }

    std::optional<Stat> maybeLstat(const CanonPath & path) override
    {
        if (!path.isRoot())
            return std::nullopt;

        std::call_once(statFlag, [this] {
            if (::fstat(fd.get(), &cachedStat) == -1)
                throw SysError("statting file '%s'", displayPrefix);
            updateMtime(cachedStat.st_mtime);
        });

        return posixStatToAccessorStat(cachedStat);
    }

    void readFile(const CanonPath & path, Sink & sink, std::function<void(uint64_t)> sizeCallback) override
    {
        if (!path.isRoot())
            throw FileNotFound("path '%s' does not exist", showPath(path));

        auto st = maybeLstat(path).value();
        off_t left = st.fileSize.value();
        off_t offset = 0;
        sizeCallback(left);

        /* TODO: Optimise for the case when Sink is an FdSink and call sendfile. Needs
           portable helper in unix/file-descriptor to handle the differences between Darwin
           and Linux. Darwin only works with sockets, while Linux can handle any destination descriptor. */

        std::array<unsigned char, 64 * 1024> buf;
        while (left) {
            checkInterrupt();
            /* N.B. Using pread for thread-safety. File pointer must not be modified. */
            ssize_t rd = pread(fd.get(), buf.data(), std::min<std::size_t>(left, buf.size()), offset);
            if (rd == -1) {
                if (errno != EINTR)
                    throw SysError("reading from file '%s'", showPath(path));
            } else if (rd == 0)
                throw SysError("unexpected end-of-file reading '%s'", showPath(path));
            else {
                assert(rd <= left);
                sink({reinterpret_cast<char *>(buf.data()), static_cast<std::size_t>(rd)});
                left -= rd;
                offset += rd;
            }
        }
    }
};

class UnixDirectorySourceAccessor : public UnixSourceAccessorBase
{
    AutoCloseFD fd;
    CanonPath rootPath;

    std::pair<Descriptor, AutoCloseFD> openParent(const CanonPath & path)
    {
        assert(!path.isRoot());
        auto parent = path.parent().value();
        if (parent.isRoot())
            return {fd.get(), {}};

        AutoCloseFD parentFdOwning =
            openFileEnsureBeneathNoSymlinks(fd.get(), parent, O_DIRECTORY | O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (!parentFdOwning && (errno == ELOOP || errno == ENOTDIR))
            throw SymlinkNotAllowed(parent);
        return {parentFdOwning.get(), std::move(parentFdOwning)};
    }

public:
    UnixDirectorySourceAccessor(AutoCloseFD fd_, CanonPath rootPath_, bool trackLastModified)
        : UnixSourceAccessorBase(trackLastModified)
        , fd(std::move(fd_))
        , rootPath(std::move(rootPath_))
    {
        displayPrefix = rootPath.abs();
    }

    std::optional<std::filesystem::path> getPhysicalPath(const CanonPath & path) override
    {
        if (path.isRoot())
            return std::filesystem::path(rootPath.abs());
        return std::filesystem::path(rootPath.abs()) / path.rel();
    }

    std::optional<Stat> maybeLstat(const CanonPath & path) override
    try {
        struct ::stat st;

        if (path.isRoot()) {
            if (::fstat(fd.get(), &st) == -1)
                return std::nullopt;
        } else {
            auto [parentFd, parentFdOwning] = openParent(path);
            if (parentFd == INVALID_DESCRIPTOR)
                return std::nullopt;
            if (::fstatat(parentFd, std::string(path.baseName().value()).c_str(), &st, AT_SYMLINK_NOFOLLOW) == -1)
                return std::nullopt;
        }

        updateMtime(st.st_mtime);
        return posixStatToAccessorStat(st);
    } catch (SymlinkNotAllowed & e) {
        throw SymlinkNotAllowed(e.path, "path '%s' is a symlink", showPath(e.path));
    }

    void readFile(const CanonPath & path, Sink & sink, std::function<void(uint64_t)> sizeCallback) override
    try {
        if (path.isRoot())
            throw NotARegularFile("'%s' is not a regular file", showPath(path));

        AutoCloseFD fileFd = openFileEnsureBeneathNoSymlinks(fd.get(), path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (!fileFd) {
            if (errno == ELOOP) /* The last component is a symlink. */
                throw NotARegularFile("'%s' is a symlink, not a regular file", showPath(path));
            if (errno == ENOENT || errno == ENOTDIR) /* Intermediate component might not exist. */
                throw FileNotFound("file '%s' does not exist", showPath(path));
            throw SysError("opening '%s'", showPath(path));
        }

        UnixFileSourceAccessor fileAccessor(std::move(fileFd), rootPath / path, trackLastModified);
        fileAccessor.readFile(CanonPath::root, sink, sizeCallback);

        if (auto fileMtime = fileAccessor.getLastModified())
            mtime = std::max(mtime, *fileMtime);
    } catch (SymlinkNotAllowed & e) {
        throw SymlinkNotAllowed(e.path, "path '%s' is a symlink", showPath(e.path));
    }

    DirEntries readDirectory(const CanonPath & path) override
    try {
        AutoCloseFD dirFdOwning;

        if (path.isRoot()) {
            /* Get a fresh file descriptor for thread-safety. */
            dirFdOwning = ::openat(fd.get(), ".", O_DIRECTORY | O_RDONLY | O_CLOEXEC);
        } else {
            dirFdOwning =
                openFileEnsureBeneathNoSymlinks(fd.get(), path, O_DIRECTORY | O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        }

        if (!dirFdOwning) {
            if (errno == ENOTDIR)
                throw NotADirectory("'%s' is not a directory", showPath(path));
            throw SysError("opening directory '%s'", showPath(path));
        }

        AutoCloseDir dir(::fdopendir(dirFdOwning.get()));
        if (!dir)
            throw SysError("opening directory '%s'", showPath(path));
        dirFdOwning.release();

        DirEntries entries;
        const ::dirent * dirent = nullptr;

        while (errno = 0, dirent = ::readdir(dir.get())) {
            checkInterrupt();
            std::string_view name(dirent->d_name);
            if (name == "." || name == "..")
                continue;

            std::optional<Type> type;
            switch (dirent->d_type) {
            case DT_REG:
                type = tRegular;
                break;
            case DT_DIR:
                type = tDirectory;
                break;
            case DT_LNK:
                type = tSymlink;
                break;
            case DT_CHR:
                type = tChar;
                break;
            case DT_BLK:
                type = tBlock;
                break;
            case DT_FIFO:
                type = tFifo;
                break;
            case DT_SOCK:
                type = tSocket;
                break;
            default:
                type = std::nullopt;
                break;
            }
            entries.emplace(name, type);
        }

        if (errno)
            throw SysError("reading directory %1%", path);

        return entries;
    } catch (SymlinkNotAllowed & e) {
        throw SymlinkNotAllowed(e.path, "path '%s' is a symlink", showPath(e.path));
    }

    std::string readLink(const CanonPath & path) override
    try {
        if (path.isRoot())
            throw NotASymlink("file '%s' is not a symlink", showPath(path));

        auto [parentFd, parentFdOwning] = openParent(path);
        if (parentFd == INVALID_DESCRIPTOR)
            throw FileNotFound("file '%s' does not exist", showPath(path));

        try {
            return readLinkAt(parentFd, CanonPath(path.baseName().value()));
        } catch (SysError & e) {
            if (e.errNo == EINVAL)
                throw NotASymlink("file '%s' is not a symlink", showPath(path));
            throw;
        }
    } catch (SymlinkNotAllowed & e) {
        throw SymlinkNotAllowed(e.path, "path '%s' is a symlink", showPath(e.path));
    }
};

} // namespace unix
} // namespace nix
