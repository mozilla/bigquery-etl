# M1 Mac Setup Guide

This guide should get you set up to run `bigquery-etl` on an M1 or M2 Mac. The
reason you need to take some extra steps here is because M1/M2 Macs run on
Apple CPUs that use ARM architecture, as opposed to what older Macs use, Intel
CPUs on x86 architecture. There are currently some compatibility issues between
some of our dependencies and ARM. To get around this, we'll use Rosetta, which
is an x86 emulator.

You're free to set this up however you like, but in this guide we'll set up 2
parallel Homebrew versions, one for ARM and one for x86 (in Rosetta), create
aliases for the two, and install all of `bigquery-etl`'s dependencies in
Rosetta mode.

For tips on maintaining parallel stacks of python and homebrew running with and without Rosetta, see blog posts from [Thinknum](https://medium.com/thinknum/how-to-install-python-under-rosetta-2-f98c0865e012) and [Sixty North](http://sixty-north.com/blog/pyenv-apple-silicon.html).

> :exclamation: These instructions will install everything in Rosetta mode and
> assume that you can do all your work in Rosetta mode.
>
> If you need to work with other repos outside of Rosetta mode (esp in Python)
> these steps are probably not right for you -- ask in #data-help for advice on
> getting set up

1. Install Rosetta if you don't have it yet

   ```zsh
   softwareupdate --install-rosetta
   ```

2. Set Terminal to run in Rosetta mode:
   1. In Finder, go to Applications -> Utilities
   2. Right-click on Terminal and choose Get Info
   3. Check the box "Open using Rosetta"
   4. Open a new Terminal session and run `arch`. This will show you the
      current architecture -- it should output `i386`. If you're not in Rosetta
      mode, it will output `arm64`
   5. (Optional) You can add the current architecture to your prompt -- add the
      following to your `~/.zshrc` or `/.bashrc`:

      ```zsh
      setopt PROMPT_SUBST
      PROMPT='$(arch) %~ %# '
      ```

3. Install Homebrew:
   1. Still in your Rosetta mode shell, follow the instructions at
      <https://brew.sh/> to install Homebrew
   2. This will have installed Homebrew to `/usr/local/bin/brew`; now we'll add
      some aliases -- add the following to your `~/.zshrc` or `/.bashrc`:

      ```zsh
      alias brew86='/usr/local/bin/brew'
      alias brewm1='/opt/homebrew/bin/brew'
      alias brew='echo "Use brew86 or brewm1"'
      ```

      You'll now use `brew86` to install things to Rosetta Homebrew and
      `brewm1` to install to ARM Homebrew. The third alias I added because I
      kept just typing `brew` out of habit without knowing which one I was
      using
   3. Delete the following line from your `~/.zprofile` or `~/bash_profile` if
      you previously added it:

      ```zsh
      eval "$(/opt/homebrew/bin/brew shellenv)"
      ```

4. (Recommended) Clear out non-Rosetta versions of software that's used in
   `bigquery-etl`:
   1. If you installed Python 3.10 via pyenv, uninstall it with

      ```zsh
      pyenv uninstall 3.10.6  # (or whichever version you have installed)
      ```

   2. If you installed pyenv, uninstall it:

      ```zsh
      brewm1 uninstall pyenv
      ```

   3. If you're using tmux or something like that, uninstall that too and
      reinstall via `brew86`
5. Install the core `bigquery-etl` dependencies via `brew86`:

   ```zsh
   brew86 install pyenv
   ```

6. Install python; you'll need to install `xz` first

   ```zsh
   brew86 install xz
   pyenv install 3.10.6  # (or whichever version)
   ```

You should now be set to run through the [`bigquery-etl` setup instructions](README.md#quick-start)
