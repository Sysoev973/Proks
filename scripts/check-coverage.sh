#!/usr/bin/env sh
# Fails when a package's statement coverage in a Go cover profile is below its
# threshold. Usage: scripts/check-coverage.sh coverage.out thresholds.txt
#
# The thresholds file has one "<package> <percent>" pair per line; lines
# starting with # are comments. Packages without a threshold are reported
# but never fail the check.
set -eu

profile=${1:?usage: check-coverage.sh <profile> <thresholds>}
thresholds=${2:?usage: check-coverage.sh <profile> <thresholds>}

awk '
FNR == NR {
	if ($0 !~ /^[[:space:]]*(#|$)/) want[$1] = $2
	next
}
FNR == 1 && /^mode:/ { next }
{
	split($1, loc, ":")
	pkg = loc[1]
	sub(/\/[^\/]*$/, "", pkg)
	block = $1
	# The same block can appear more than once (e.g. -coverpkg); count it once.
	if (!(block in seen)) {
		seen[block] = 1
		total[pkg] += $2
	}
	if ($3 > 0 && !(block in hit)) {
		hit[block] = 1
		covered[pkg] += $2
	}
}
END {
	failed = 0
	for (pkg in total) {
		pct = total[pkg] ? 100 * covered[pkg] / total[pkg] : 100
		status = "    "
		if (pkg in want) {
			status = pct + 0.0001 >= want[pkg] ? "ok  " : "FAIL"
			if (status == "FAIL") failed = 1
			printf "%s %-36s %6.1f%% (min %s%%)\n", status, pkg, pct, want[pkg]
		} else {
			printf "%s %-36s %6.1f%%\n", status, pkg, pct
		}
	}
	for (pkg in want) {
		if (!(pkg in total)) {
			printf "FAIL %-36s no coverage data\n", pkg
			failed = 1
		}
	}
	exit failed
}
' "$thresholds" "$profile"
