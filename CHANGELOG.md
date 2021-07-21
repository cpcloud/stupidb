# Release Notes

<!--next-version-placeholder-->

## v3.1.0 (2021-07-21)
### Feature
* **nixpkgs:** Bump ([`26e49c6`](https://github.com/cpcloud/stupidb/commit/26e49c64e12af79c916bc4ec8fea9f174164c191))

### Documentation
* **build:** Remove watchdog functionality for now ([`3f2de29`](https://github.com/cpcloud/stupidb/commit/3f2de29bd6436eff1d01630bf819344019e3e8c0))
* **typo:** Fix typo ([`0d0129f`](https://github.com/cpcloud/stupidb/commit/0d0129f576341d861b0e45c12ab8a0b089453028))
* **example:** Fix animation to match prose ([`2e91685`](https://github.com/cpcloud/stupidb/commit/2e91685634b5c9c38e983a6ad1202d625c460060))
* **contributing:** Fix some typos ([`4304348`](https://github.com/cpcloud/stupidb/commit/43043480d922f89299ac3158c3f83ccafafd57ee))
* **animation:** Add animation feature to docs build requirements.txt ([`3c21a96`](https://github.com/cpcloud/stupidb/commit/3c21a969d56d27fba91889ac73626566e0594d30))
* **animate:** Update animation with new font ([`9f2e6da`](https://github.com/cpcloud/stupidb/commit/9f2e6da147821d4331dbde79850e971051f0ffe7))
* **build:** Fix doc builds ([`a339b63`](https://github.com/cpcloud/stupidb/commit/a339b636af670ed00fca2a96b79e935d6ad43a0d))

## v3.0.1 (2021-07-20)
### Documentation
* **build:** Make docs deps extras ([`c11679a`](https://github.com/cpcloud/stupidb/commit/c11679a95bfdc11f351e173540d8106a18898b22))
* **readme:** Update CI badge ([`8b77999`](https://github.com/cpcloud/stupidb/commit/8b779999839d5a0b8c6d31cb789a15f381ef5484))

### Performance
* **core:** Actually use cytoolz ([`8d67203`](https://github.com/cpcloud/stupidb/commit/8d67203a23436b2320aecf1d375c5e5b1efcf74d))

## v3.0.0 (2021-07-20)
### Feature
* **python-version:** Support python3.7 ([`e52b95f`](https://github.com/cpcloud/stupidb/commit/e52b95fb91e288fcc2f7156433c609a6e39928bb))

### Fix
* **protocols.py:** Fix type vars in protocols.py ([`8693913`](https://github.com/cpcloud/stupidb/commit/8693913daf3426afc943a97048764a5aaf7fb1fe))
* **associative.py:** Fix combine location in AssociativeAggregate subclasses ([`2b1da5d`](https://github.com/cpcloud/stupidb/commit/2b1da5d866df850aac1d00bcb18919954babc190))
* **tests:** Fix typehints in test ([`5253a8d`](https://github.com/cpcloud/stupidb/commit/5253a8dd441dcab69a3aea34401eba69fac8deac))
* **dev:** Add niv to nix-shell ([`26b78b4`](https://github.com/cpcloud/stupidb/commit/26b78b4a93b314ed81b0b0ed9563d42ced6b765b))

### Breaking
* Animation users will break  ([`c0f81c8`](https://github.com/cpcloud/stupidb/commit/c0f81c838278f2cddc873cf6fe5266205233b6d9))

## v2.0.0 (2021-07-20)
### Breaking
* Code wasn't broken, but this is considered breaking because the process by which a person installs the code breaks  ([`dbc67f7`](https://github.com/cpcloud/stupidb/commit/dbc67f7324585c7f0e61c6ffb4467297f57f8f32))

### Documentation
* **dev:** Rewrite docs on installation and setup ([`dbc67f7`](https://github.com/cpcloud/stupidb/commit/dbc67f7324585c7f0e61c6ffb4467297f57f8f32))
* **servedocs:** Install dev dependencies for running make servedocs ([`5c21042`](https://github.com/cpcloud/stupidb/commit/5c210427a13a8acb68f92d1ab16bb336eca1c2b4))
* **build:** Update generated docs ([`50b0b3c`](https://github.com/cpcloud/stupidb/commit/50b0b3c9496859b143d2ca5a670f71f54a16f209))
* **api.py:** Fix doctests in api.py ([`9189bc5`](https://github.com/cpcloud/stupidb/commit/9189bc531f5bf96ff204a5d0521d1634017c78f0))
* **build:** Use semantic-release to set versions in docs/conf.py ([`54f7f84`](https://github.com/cpcloud/stupidb/commit/54f7f84929739274a7166c7db7bb42d0249c50cd))
* Remove history.rst ([`7cfd641`](https://github.com/cpcloud/stupidb/commit/7cfd6412ab9d07a9ce58b43c6cff95932519f8a9))
* Replace stupidb.stupidb.rst with stupidb.core.rst ([`9b2c4d0`](https://github.com/cpcloud/stupidb/commit/9b2c4d047757934c89f486ccad056bd96153f05f))
* Use pip for docs install ([`7f07b21`](https://github.com/cpcloud/stupidb/commit/7f07b2176fa0648547a14fa994efa4fdde7177ee))
* **build:** Don't use hashes ([`3e72389`](https://github.com/cpcloud/stupidb/commit/3e7238909f6176bb1cb16925e70c064de061eace))
* Remove history section ([`5dbf64f`](https://github.com/cpcloud/stupidb/commit/5dbf64f1abadfc49b89df6462f686e8fbd34007d))
* Use requirements.txt for readthedocs ([`3ecd05c`](https://github.com/cpcloud/stupidb/commit/3ecd05cc900e308c5876533c12b84ec043579bd4))
* Better release notes header ([`7ff5707`](https://github.com/cpcloud/stupidb/commit/7ff5707c46e0cd1fac1b49bb0d8ba9fa31838f5b))

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
