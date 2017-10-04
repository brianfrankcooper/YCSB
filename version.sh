#!/bin/bash
  #
  # Script to help generate a new release candidate.  This assumes that
  # the master branch is checked out and the working directory is clean.
  #
  # To confirm changes before checking them in, replace the -A option
  # to "git add" below with -p.
  #

  # Update the following two variables before running this script.
  VERSION=0.13.0
  RC_NUM=1

  RC_NUMBER=RC${RC_NUM}
  NEXT_RC_NUMBER=RC$((${RC_NUM} + 1))
  RC_VERSION=${VERSION}-${RC_NUMBER}

  git checkout ${VERSION}-staging

  # Modify version string from SNAPSHOT to RC.
  test $RC_NUM = 1 && FROM=${VERSION}-SNAPSHOT || FROM=${RC_VERSION}-SNAPSHOT
  TO=$RC_VERSION

  FILES="`grep -rl -- -SNAPSHOT *`"

  echo "$FILES" | xargs sed -i '' -e "s/$FROM/$TO/"
  git add -A
  git commit -m "[release] mark ${VERSION} ${RC_NUMBER}"

  # Now create a tag that we can reference on GitHub.
  git tag $RC_VERSION

  # Modify version string from RC to the next RC snapshot.  Now fixes
  # that come in during testing can be picked back here for files that
  # need updating.
#  FROM=$RC_VERSION
#  TO=${VERSION}-${NEXT_RC_NUMBER}-SNAPSHOT
#  echo "$FILES" | xargs sed -i '' -e "s/$FROM/$TO/"
#  git add -A
#  git commit -m "[release] update version for ${NEXT_RC_NUMBER}"

  # Finally push all of this up to GitHub.
#  git push origin ${VERSION}-staging $RC_VERSION
