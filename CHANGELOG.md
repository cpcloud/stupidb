# Release Notes

## [7.0.1](https://github.com/cpcloud/stupidb/compare/v7.0.0...v7.0.1) (2022-01-04)


### Bug Fixes

* ensure release notes are generated properly ([8a35d00](https://github.com/cpcloud/stupidb/commit/8a35d00dc61f6f1062ade2546654a94b3bd75fa9))

# [7.0.0](https://github.com/cpcloud/stupidb/compare/v6.1.1...v7.0.0) (2022-01-04)


### Bug Fixes

* **ci:** add changelog pluging to update CHANGELOG.md ([ab34566](https://github.com/cpcloud/stupidb/commit/ab34566fcccff7ece8965cbe21a435e413969246))
* preserve environment variables needed for poetry publish ([194d7af](https://github.com/cpcloud/stupidb/commit/194d7afb20e625ef770f387b20f81e2fad923601))


### chore

* **deps:** drop support for pythons 3.7 and 3.8 ([14d05e4](https://github.com/cpcloud/stupidb/commit/14d05e401013baccc658b02259f621754af962e0))


### BREAKING CHANGES

* **deps:** Upgrade to Python >= 3.9

## [6.1.1](https://github.com/cpcloud/stupidb/compare/v6.1.0...v6.1.1) (2022-01-04)


### Bug Fixes

* run with plain semantic-release using semantic-release/exec ([71c207e](https://github.com/cpcloud/stupidb/commit/71c207eb893f61061c57872b124be70c95453651))

## v6.1.0 (2021-12-17)
### Feature
* Add support for python 3.10 ([`36bd5fd`](https://github.com/cpcloud/stupidb/commit/36bd5fd3f12a55e3c61a532a649e76f8a5994fd2))

### Documentation
* Remove lines from README.md ([`d90eec5`](https://github.com/cpcloud/stupidb/commit/d90eec58ef18bab690d9457fe4bd949eed7f6ad7))
* Document some associative function classes ([`548b29d`](https://github.com/cpcloud/stupidb/commit/548b29d51a2e8a1e5427f1b330eb7897688228e1))
* Remove pprint usage in doctests ([`f3ac09c`](https://github.com/cpcloud/stupidb/commit/f3ac09ce6a796a43ed9ad473fe2b4403d72463d0))

## v6.0.2 (2021-12-11)
### Fix
* Remove commitlint constraint on release ([`9a73615`](https://github.com/cpcloud/stupidb/commit/9a7361554a788143d8caf46258b5266eac1d5b7a))
* Move to flakes ([`e71c661`](https://github.com/cpcloud/stupidb/commit/e71c6611320c2ceb4eda90d689b6289ce6e735af))

## v6.0.1 (2021-10-27)
### Fix
* Remove poetry-core version constraint ([`3342f49`](https://github.com/cpcloud/stupidb/commit/3342f49c6b1e5cc8e63314ed4e9bdcf7c095170a))

### Documentation
* Use autosummary for docs ([`9e189a1`](https://github.com/cpcloud/stupidb/commit/9e189a121222b24c9cbbe0dadba2053c4ea21c59))
* Don't make segment tree leaves into a doctest ([`92d7ba9`](https://github.com/cpcloud/stupidb/commit/92d7ba913af58b9716c7b9b153a8000de043e2b5))

## v6.0.0 (2021-10-13)
### Breaking
* Modules are named differently  ([`fc1bbf8`](https://github.com/cpcloud/stupidb/commit/fc1bbf8c52b8858182637112a1748e0cb2aa7dd0))

## v5.1.0 (2021-10-13)
### Feature
* Add pretty printing functionality ([`c61873c`](https://github.com/cpcloud/stupidb/commit/c61873c318f6b9d505e7be7747a1d8a907c91578))

### Fix
* Add test for arguments ([`2e344c7`](https://github.com/cpcloud/stupidb/commit/2e344c74c5e795bc417cd091939392fe4dbe636a))

## v5.0.2 (2021-09-24)


## v5.0.1 (2021-09-06)
### Performance
* Reduce the amount of new object creation if rows' ids are the same as requested ([`3afad30`](https://github.com/cpcloud/stupidb/commit/3afad30cba4a49ed3b26e95347ce0e49a851a956))

## v5.0.0 (2021-09-06)
### Breaking
* Various objects that were previously accidentally public like Relation are no longer public  ([`ea73691`](https://github.com/cpcloud/stupidb/commit/ea7369170af0a688d7370a69fa1f3dccd5a895d8))

### Documentation
* Proper license ([`437abc0`](https://github.com/cpcloud/stupidb/commit/437abc0efea08da3effdec04a7c9c8fe4d79fa61))
* Move to markdown for top-level docs ([`1f46655`](https://github.com/cpcloud/stupidb/commit/1f466555aeb945b2be6ede3a0ab75fb09053ed38))
* Update requirements.txt ([`2c07ae8`](https://github.com/cpcloud/stupidb/commit/2c07ae8d39d647a6b4dc825074c491e7406868ff))

## v4.5.0 (2021-09-05)
### Feature
* Add limit operator ([`e0e6146`](https://github.com/cpcloud/stupidb/commit/e0e6146bb2efe65fafa417089aa69d421930c00f))

## v4.4.0 (2021-07-27)
### Feature
* **nixpkgs:** Bump ([`b7c5581`](https://github.com/cpcloud/stupidb/commit/b7c558138b10178d871d388a5c5d6e3e8ca9db35))

### Documentation
* Add more docs ([`c2625dc`](https://github.com/cpcloud/stupidb/commit/c2625dc4f3c33f0eb597ff400b0b9527dfce3b67))
* **pyproject:** Add 3.8 and 3.9 metadata ([`da8365c`](https://github.com/cpcloud/stupidb/commit/da8365c983635cf00b78433730ea3845e4d1897f))

## v4.3.0 (2021-07-26)
### Feature
* **nixpkgs:** Bump ([`ef95630`](https://github.com/cpcloud/stupidb/commit/ef95630e49d66b997805b406913432e32676c1d7))

## v4.2.0 (2021-07-24)
### Feature
* **nixpkgs:** Bump ([`94509cb`](https://github.com/cpcloud/stupidb/commit/94509cbce4774c4b17dc42536064a081caeaa8de))

## v4.1.0 (2021-07-21)
### Feature
* **nixpkgs:** Bump ([`4dbafbf`](https://github.com/cpcloud/stupidb/commit/4dbafbf1eda9005d095c385fa3267dd28e1e5840))

### Documentation
* **all:** Add a sprinkling of doc strings ([`d61b03f`](https://github.com/cpcloud/stupidb/commit/d61b03fd5d45741bb1b26730b089e6c020d75481))
* **conf:** Remove intersphinx mappings from unused dependencies ([`5fe1beb`](https://github.com/cpcloud/stupidb/commit/5fe1bebde22819ad85b3680f773aaf0c822560c0))

## v4.0.0 (2021-07-21)
### Fix
* **core:** Privatize renew_id ([`2b05734`](https://github.com/cpcloud/stupidb/commit/2b057344110ae437f27362b7ccb4a1576947d971))

### Breaking
* renew_id has been change to _renew_id  ([`2b05734`](https://github.com/cpcloud/stupidb/commit/2b057344110ae437f27362b7ccb4a1576947d971))

### Performance
* **row:** Only compute hashes for each row once since their data are immutable ([`778105f`](https://github.com/cpcloud/stupidb/commit/778105f50d6f56f56c4c5985fee53f5dc8e889f0))

## v3.2.0 (2021-07-21)
### Feature
* **animate:** Add bitgraph implementation ([`9500085`](https://github.com/cpcloud/stupidb/commit/9500085bfa6e09fbb1bd264b908ac6afd4f4487d))

### Documentation
* **indextree.py:** Clean up some docs style and add more assertion error info ([`c92e3ba`](https://github.com/cpcloud/stupidb/commit/c92e3bad67e38b48756b1cfc7de90bbbc829cb3f))

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
