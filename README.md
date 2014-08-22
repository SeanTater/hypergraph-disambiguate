hypergraph-disambiguate
=======================

Word meaning disambiguation through sets

Install
------

Since it is still very experimental, this project is not available on Cabal.
However, it should be relatively easy to build anyway.

- [Install the Haskell Platform](http://www.haskell.org/platform/)
  - For yum/apt-get/brew just install "haskell-platform"
- Clone `git clone http://github.com/SeanTater/hypergraph-disambiguate && cd hypergraph-disambiguate`
- Compile: `cabal install` (takes a few minutes)
- Run: `hypergraph-disambiguate wordcounts-tiny.db` in whichever folder the previous command told you.

Far more data is available but it can all be generated from the Wikipedia database dumps.
More to come on that later.
