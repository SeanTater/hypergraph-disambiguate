hypergraph-disambiguate
=======================

Word meaning disambiguation through sets

Install
------

Since it is still very experimental, this project is not available on Cabal.
However, it should be relatively easy to build anyway.

- [Install the Haskell Platform](http://www.haskell.org/platform/)
  - For yum/apt-get/brew just install "haskell-platform"
- Upgrade Cabal if necessary (some repositories have old versions; the official site is already fresh)
  - To upgrade: `cabal update && cabal install cabal-install`
  - To update the executable path (on Linux; others vary): `echo "\nexport PATH=~/.cabal/bin:$PATH\n" >>~/.bashrc`
  - To start a new terminal with the new path: `bash`
- Clone `git clone http://github.com/SeanTater/hypergraph-disambiguate && cd hypergraph-disambiguate`
- Get the native-library dependencies: sqlite-devel, pcre-devel, and bzip2-devel
  - All available in the fedora repositories; 
- Compile: `cabal -O install` (takes a few minutes)
- Run: `hypergraph-disambiguate wordcounts-tiny.db` in whichever folder the previous command told you.

Far more data is available but it can all be generated from the Wikipedia database dumps.

- Download the top enwiki entry from [Wikipedia Database Downloads](http://meta.wikimedia.org/wiki/Data_dump_torrents#enwiki)
- Make a copy of the database with the naming convention: `cp wordcounts-tiny.db wordcounts.db`
- Extract it into an SQLite database using `bz2cat THE-DOWNLOADED-ARCHIVE | ./WikiExtractor.py`
- The result should be about 11GB uncompressed (much smaller than the uncompressed archive)
