#include "nix/util/memory-source-accessor.hh"
#include "nix/util/source-accessor.hh"
#include "nix/util/unix-source-accessor.hh"

namespace nix {

using namespace nix::unix;

ref<SourceAccessor> getFSSourceAccessor()
{
    static auto rootFS =
        make_ref<UnixDirectorySourceAccessor>(openDirectory("/"), CanonPath("/"), /*trackLastModified=*/false);
    rootFS->displayPrefix.clear();
    return rootFS;
}

ref<SourceAccessor> makeFSSourceAccessor(std::filesystem::path root, bool trackLastModified)
{
    using namespace unix;

    if (root.empty())
        return getFSSourceAccessor();

    assert(root.is_absolute());
    auto rootPath = CanonPath(root.native());
    if (rootPath.isRoot())
        return getFSSourceAccessor();

    assert(rootPath.abs().starts_with("/")); /* In case the invariant is broken somehow. */

    AutoCloseFD fd(::open(rootPath.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (!fd) {
        auto makeDummySourceAccessor = [&rootPath]() {
            auto accessor = make_ref<MemorySourceAccessor>();
            accessor->setPathDisplay(rootPath.abs());
            return accessor;
        };

        if (errno == ELOOP) /* Opening a symlink, can read it straight into memory source accessor. */ {
            auto parent = rootPath.parent().value(); /* Always present, isRoot is handled above. */
            auto name = std::string(rootPath.baseName().value());
            AutoCloseFD parentFd(::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
            if (!parentFd)
                return makeDummySourceAccessor();

            struct ::stat st;
            if (::fstatat(parentFd.get(), name.c_str(), &st, AT_SYMLINK_NOFOLLOW) == -1)
                return makeDummySourceAccessor();

            auto target = readLinkAt(parentFd.get(), CanonPath(name));

            class SymlinkSourceAccessor : public MemorySourceAccessor
            {
                bool trackLastModified;
                std::time_t mtime;

            public:
                SymlinkSourceAccessor(std::string target, CanonPath rootPath, bool trackLastModified, std::time_t mtime)
                    : trackLastModified(trackLastModified)
                    , mtime(mtime)
                {
                    MemorySink sink{*this};
                    sink.createSymlink(CanonPath::root, target);
                    displayPrefix = rootPath.abs();
                }

                std::optional<std::time_t> getLastModified() override
                {
                    return trackLastModified ? std::optional{mtime} : std::nullopt;
                }

                std::string showPath(const CanonPath & path) override
                {
                    /* When rendering the file itself omit the trailing slash. */
                    return path.isRoot() ? displayPrefix : SourceAccessor::showPath(path);
                }
            };

            return make_ref<SymlinkSourceAccessor>(
                std::move(target), std::move(rootPath), trackLastModified, st.st_mtime);
        }

        return makeDummySourceAccessor(); /* Return a dummy accessor, errors should be thrown when accessing files and
                                             not at construction time. */
    }

    struct ::stat st;
    if (::fstat(fd.get(), &st) == -1)
        throw SysError("statting '%s'", rootPath.abs());

    if (S_ISDIR(st.st_mode)) {
        return make_ref<UnixDirectorySourceAccessor>(std::move(fd), std::move(rootPath), trackLastModified);
    } else if (S_ISREG(st.st_mode)) {
        return make_ref<UnixFileSourceAccessor>(std::move(fd), std::move(rootPath), trackLastModified, &st);
    } else {
        return makeEmptySourceAccessor();
    }
}

} // namespace nix
