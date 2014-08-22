hypergraph-disambiguate
=======================

Word meaning disambiguation through sets

Install
------

Since it is still very experimental, this project is not available on Cabal.
However, it should be relatively easy to build anyway.

- [Install the Haskell Platform](http://www.haskell.org/platform/) (which took me about 5 min)
  - For yum/apt-get/brew it should be available as "haskell-platform" - tends to be very automatic
- Clone this repository: `git clone http://github.com/SeanTater/hypergraph-disambiguate && cd hypergraph-disambiguate`
- Setup dependencies and install `cabal install` (takes a few minutes to download stuff)
- Run `hypergraph-disambiguate wordcounts-tiny.db` in whichever folder the previous command told you.

Far more data is available but it can all be generated from the Wikipedia database dumps.
More to come on that later.
