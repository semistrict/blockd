#include <sys/ioctl.h>
#include <linux/userfaultfd.h>
#include <stddef.h>

/* Keep the hand-written Rust ABI in sync with the build host's kernel headers. */
_Static_assert(UFFDIO_API == 0xc018aa3fUL, "UFFDIO_API changed");
_Static_assert(UFFDIO_REGISTER == 0xc020aa00UL, "UFFDIO_REGISTER changed");
_Static_assert(UFFDIO_WAKE == 0x8010aa02UL, "UFFDIO_WAKE changed");
_Static_assert(UFFDIO_COPY == 0xc028aa03UL, "UFFDIO_COPY changed");
_Static_assert(UFFDIO_WRITEPROTECT == 0xc018aa06UL, "UFFDIO_WRITEPROTECT changed");
_Static_assert(UFFDIO_CONTINUE == 0xc020aa07UL, "UFFDIO_CONTINUE changed");

_Static_assert(UFFD_FEATURE_PAGEFAULT_FLAG_WP == (1ULL << 0),
               "UFFD_FEATURE_PAGEFAULT_FLAG_WP changed");
_Static_assert(UFFD_FEATURE_MINOR_SHMEM == (1ULL << 10),
               "UFFD_FEATURE_MINOR_SHMEM changed");
_Static_assert(UFFD_FEATURE_WP_HUGETLBFS_SHMEM == (1ULL << 12),
               "UFFD_FEATURE_WP_HUGETLBFS_SHMEM changed");
_Static_assert(UFFD_FEATURE_WP_UNPOPULATED == (1ULL << 13),
               "UFFD_FEATURE_WP_UNPOPULATED changed");

_Static_assert(sizeof(struct uffd_msg) == 32, "struct uffd_msg size changed");
_Static_assert(offsetof(struct uffd_msg, event) == 0,
               "uffd_msg.event offset changed");
_Static_assert(offsetof(struct uffd_msg, arg.pagefault.flags) == 8,
               "uffd_msg.pagefault.flags offset changed");
_Static_assert(offsetof(struct uffd_msg, arg.pagefault.address) == 16,
               "uffd_msg.pagefault.address offset changed");

int main(void) {
    return 0;
}
