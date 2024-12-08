# Commits message naming

> [!NOTE]
> Long story short, we use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) together with an emoji corresponding to the commit type.

## RadishDB's Conventional Commits

See how [a minor change](#examples) to your commit message style can make a difference.

> [!TIP]
> Have a look at **[git-conventional-commits](https://github.com/qoomon/git-conventional-commits)** , a CLI util to ensure these conventions, determine version and generate changelogs

## Commit Message Formats

### Default

<pre>
<b><a href="#types">&lt;type&gt;</a></b></font>(<b><a href="#scopes">&lt;optional scope&gt;</a></b>): <b><a href="#emojis">&lt;:emoji:&gt;</a></b> <b><a href="#description">&lt;description&gt;</a></b>
<sub>empty separator line</sub>
<b><a href="#body">&lt;optional body&gt;</a></b>
<sub>empty separator line</sub>
<b><a href="#footer">&lt;optional footer&gt;</a></b>
</pre>

### Merge Commit

<pre>
Merge branch '<b>&lt;branch name&gt;</b>'
</pre>
<sup>Follows default git merge message</sup>

### Revert Commit

<pre>
Revert "<b>&lt;reverted commit subject line&gt;</b>"
</pre>
<sup>Follows default git revert message</sup>

OR

<pre>
revert: :back: <b>&lt;description&gt;</b>
<sub>empty separator line</sub>
<b>"&lt;reverted commit subject line&gt;"</b>
</pre>

### Initial Commit

```git
init: :dizzy: initial commit
```

### Types

* API relevant changes
  * `feat` Commits, that adds or remove a new feature
  * `fix` Commits, that fixes a bug
* `refactor` Commits, that rewrite/restructure your code, however does not change any API behaviour
  * `perf` Commits are special `refactor` commits, that improve performance
* `style` Commits, that do not affect the meaning (white-space, formatting, missing semi-colons, etc)
* `test` Commits, that add missing tests or correcting existing tests
* `docs` Commits, that affect documentation only
* `build` Commits, that affect build components like build tool, ci pipeline, dependencies, project version, ...
* `ops` Commits, that affect operational components like infrastructure, deployment, backup, recovery, ...
* `security` Commits, that affect security like fixing CVEs, CVSSs, ...
* `chore` Miscellaneous commits e.g. modifying `.gitignore`

### Scopes

The `scope` provides additional contextual information.

* Is an **optional** part of the format
* Allowed Scopes depends on the specific project
* Don't use issue identifiers as scopes

### Breaking Changes Indicator

Breaking changes should be indicated by an `!` before the `:` in the subject line e.g. `feat(api)!: remove status endpoint`

* Is an **optional** part of the format

### Description

The `description` contains a concise description of the change.

* Is a **mandatory** part of the format
* Use the imperative, present tense: "change" not "changed" nor "changes"
  * Think of `This commit will...` or `This commit should...`
* Don't capitalize the first letter
* No dot (`.`) at the end

### Body

The `body` should include the motivation for the change and contrast this with previous behavior.

* Is an **optional** part of the format
* Use the imperative, present tense: "change" not "changed" nor "changes"
* This is the place to mention issue identifiers and their relations

### Footer

The `footer` should contain any information about **Breaking Changes** and is also the place to **reference Issues** that this commit refers to.

* Is an **optional** part of the format
* **optionally** reference an issue by its id.
* **Breaking Changes** should start with the word `BREAKING CHANGES:` followed by space or two newlines. The rest of the commit message is then used for this.

## Emojis

| Style | Description | Emoji |
| --- | --- | --- |
| `build: :building_construction: msg` | Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm) | :building_construction: |
| `chore: :wrench: msg` | Other changes that don't modify src or test files | :wrench: |
| `chore(deps): :link: msg` | Update or add new dependency | :link: |
| `chore(release): :rocket: msg` | Release | :rocket: |
| `ci: :construction_worker: msg` | Changes to our CI configuration files and scripts | :construction_worker: |
| `cut: :scissors: msg` | Cut something from project | :scissors: |
| `docs: :books: msg` | Documentation only changes | :books: |
| `feat: :sparkles: msg` | A new feature | :sparkles: |
| `fix: :bug: msg` | A bug fix | :bug: |
| `fix(ci): :bug: :construction_worker: msg` | Fix CI config | :bug: :construction_worker: |
| `fix(docs): :bug: :books: msg` | Fix documentation (typo, etc) | :bug: :books: |
| `fix(test): :bug: :rotating_light: msg` | Fix tests | :bug: :rotating_light: |
| `init: :dizzy: msg` | Initial commit |  :dizzy: |
| `linter: :nail_care: msg` | Update linters config | :nail_care: |
| `perf: :chart_with_upwards_trend: msg` | A code change that improves performance | :chart_with_upwards_trend: |
| `refactor: :recycle: msg` | A code change that neither fixes a bug nor adds a feature | :recycle: |
| `revert: :back: msg` | Reverts a previous commit | :back: |
| `security: :shield: msg` | Fix some security vulnerability | :shield: |
| `style: :art: msg` | Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc) | :art: |
| `test: :rotating_light: msg` | Adding missing tests or correcting existing tests | :rotating_light: |

### Examples

* ```git
  feat: :sparkles: add email notifications on new direct messages
  ```
* ```git
  feat(shopping cart): :sparkles: add the amazing button
  ```
* ```git
  feat!: :sparkles: remove ticket list endpoint

  refers to JIRA-1337

  BREAKING CHANGES: ticket enpoints no longer supports list all entites.
  ```
* ```git
  fix(shopping-cart): :bug: prevent order an empty shopping cart
  ```
* ```git
  fix(api): :bug: fix wrong calculation of request body checksum
  ```
* ```git
  fix: :bug: add missing parameter to service call

  The error occurred because of <reasons>.
  ```
* ```git
  perf: :chart_with_upwards_trend: decrease memory footprint for determine uniqe visitors by using HyperLogLog
  ```
* ```git
  build: :building_construction: update dependencies
  ```
* ```git
  chore(release): :rocket: bump version to 1.0.0
  ```
* ```git
  refactor: :recycle: implement fibonacci number calculation as recursion
  ```
* ```git
  style: :art: remove empty line
  ```

-----

## References

* https://www.conventionalcommits.org/
* https://github.com/angular/angular/blob/master/CONTRIBUTING.md
* http://karma-runner.github.io/1.0/dev/git-commit-msg.html
<br>

* https://github.com/github/platform-samples/tree/master/pre-receive-hooks
* https://github.community/t5/GitHub-Enterprise-Best-Practices/Using-pre-receive-hooks-in-GitHub-Enterprise/ba-p/13863
