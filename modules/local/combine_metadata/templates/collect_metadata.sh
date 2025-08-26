#!/bin/bash

# Save metadata to a JSON file
cat <<-END_METADATA > metadata.json
${json.toPrettyString()}
END_METADATA

# Run validation script
validate_metadata.py \\
    metadata.json \\
    $args \\
    --logfile metadata.log

cat <<-END_VERSIONS > versions.yml
"${task.process}":
    python: \$(python --version | awk '{ print \$2 }')
    pandas: \$(python -c "import pandas; print(pandas.__version__)")
END_VERSIONS