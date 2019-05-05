# How to contribute

This document describes the basic workflow for contributing to TiKV, and what is
expected from contributors submitting pull requests. Reading and following this
guide will make it easier to get your contribution accepted. For deeper detail
about hacking on TiKV see the [TiKV Development Guide][dev-guide].

[dev-guide]: docs/development.md

## Getting started

- Fork the repository on GitHub.
- Read the README.md for build instructions.
- Play with the project, submit bugs, submit patches!

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Create a topic branch from where you want to base your work. This is usually master.
- Make commits of logical units and add test cases if the change fixes a bug or adds new functionality.
- Run tests and make sure all the tests pass.
- Make sure your commit messages are in the proper format and signed (see below).
- Push your changes to a topic branch in your fork of the repository.
- Submit a pull request.
- Work with reviewers until it is approved by two maintainers, indicated by comments saying "LGTM" (looks good to me).

Thanks for your contributions!

### Code style

We follow the coding style enforced by [rustfmt](https://github.com/rust-lang/rustfmt) and [clippy](https://github.com/rust-lang/rust-clippy). Note that we use custom clippy rules that are enforced through the `make format` and `make clippy` Makefile rules, both of which are run by `make dev`.

Please follow this style to make TiKV easy to review, maintain and develop.

### Format of the Commit Message

We follow a rough convention for commit messages that is designed to answer two
questions: what changed and why. The subject line should feature the what and
the body of the commit should describe the why.

```
engine/raftkv: add comment for variable declaration.

Improve documentation.
```

The format can be described more formally as follows:

```
<subsystem>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<footer>(optional)
```

The first line is the subject and should be no longer than 70 characters, the
second line is always blank, and other lines should be wrapped at 80 characters.
This allows the message to be easier to read on GitHub as well as in various
git tools.

If the change affects more than one subsystem, you can use comma to separate them like `util/codec,util/types:`.

If the change affects many subsystems, you can use ```*``` instead, like ```*:```.

For simple commits with obvious reasoning, it can be ok to leave the body of the commit message blank.

### Signing off the Commit

Commits require a [Developer Certificate of Origin](https://developercertificate.org/) and so commits must contain a "Signed-off-by" line. You can use the `-s` option to `git commit` to automatically add a `Signed-off-by` to the commit message.
