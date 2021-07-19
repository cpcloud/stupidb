# Release Notes

<!--next-version-placeholder-->

## v1.0.1 (2021-07-19)
### Performance
* Use faster to run but slower to build because we now have caching ([`f2d71ac`](https://github.com/cpcloud/stupidb/commit/f2d71acb4011295b02e4229618c5f7c2a7a937a1))

## v1.0.0 (2021-07-19)
### Feature
* **dev:** Add the ability to use conda with nix ([`830ec2c`](https://github.com/cpcloud/stupidb/commit/830ec2cd61d2eaad0c71118c8b146adb67d68f63))
* **dev:** Use nix and poetry for development ([`a76a5f3`](https://github.com/cpcloud/stupidb/commit/a76a5f3001c196058c96ac706ab0dce727b77b83))

### Fix
* **shell.nix:** Fix import ([`269447d`](https://github.com/cpcloud/stupidb/commit/269447d0f3f0d50719aec9388172b4198801d069))
* **setup.cfg:** Remove quotes for current_version ([`64202ef`](https://github.com/cpcloud/stupidb/commit/64202ef07f717d5cb8a5ec6b2859aa39033d7b95))
* **setup.cfg:** Don't quote commit message ([`516fdfa`](https://github.com/cpcloud/stupidb/commit/516fdfab2f80a6f6af2a71fa47eec230c09e9563))
* **animation:** Update api usage for networkx update ([`4005e2e`](https://github.com/cpcloud/stupidb/commit/4005e2e00066e981bb63216b35ad0f6286be2db4))

### Breaking
* networkx had a breaking change by removing the .node property on DiGraph  ([`4005e2e`](https://github.com/cpcloud/stupidb/commit/4005e2e00066e981bb63216b35ad0f6286be2db4))
* Avoids module inception  ([`61df2c1`](https://github.com/cpcloud/stupidb/commit/61df2c1aee1f35ac1b3dd385aa8b03b06883acb8))

### Documentation
* **pyproject.toml:** Add commentary about why we exclude tests/ ([`4abcdb1`](https://github.com/cpcloud/stupidb/commit/4abcdb183b4c613fe587762c4479d0a72d8d48da))

### Performance
* Use slower to run but faster to build mypy ([`65fa71b`](https://github.com/cpcloud/stupidb/commit/65fa71b182150741f9266c3d9e2106f3398e1c9d))
