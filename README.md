# timer-queue

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE-APACHE)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE-MIT)
[![License: Zlib](https://img.shields.io/badge/License-Zlib-blue.svg)](LICENSE-ZLIB)

A Linux virtual filesystem for manipulating commits in a [jj](https://www.jj-vcs.dev/) repository.

Unlike a regular jj working copy, a VFS allows instantly checking out any revision of an arbitrarily
large repository without waiting for files to be copied around. I/O is bridged directly to the
repository's storage backend.

```sh
echo Y | sudo tee /sys/module/fuse/parameters/enable_uring
# Mount a repository
cargo run -- /path/to/repo /path/to/mountpoint &
# Access files through the mount point
cat /path/to/mountpoint/files
```

## License

Licensed under any of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
 * Zlib license ([LICENSE-ZLIB](LICENSE-ZLIB) or
   https://opensource.org/licenses/Zlib)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
triple licensed as above, without any additional terms or conditions.
