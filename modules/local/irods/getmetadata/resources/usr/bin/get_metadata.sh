#!/usr/bin/env bash

resource_type=$1
resource_path=$2

if [[ -z "$resource_type" || -z "$resource_path" ]]; then
    echo "Usage: $0 <resource_type> <resource_path>"
    exit 1
fi

imeta ls $resource_type "$resource_path" | \
    grep -E "attribute|value" | \
    sed -e 's/^attribute: //' -e 's/^value: //' | \
    awk '
        NR%2==1 { keys[++n] = $0 }
        NR%2==0 { values[n] = $0 }
        END {
            for(i=1; i<=n; i++) {
                printf "%s%s", keys[i], (i<n ? "\t" : "\n")
            }
            for(i=1; i<=n; i++) {
                printf "%s%s", values[i], (i<n ? "\t" : "\n")
            }
        }
    '