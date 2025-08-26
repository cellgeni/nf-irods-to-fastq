process IRODS_FIND {
    tag "Looking for a file/collection with matching metadata for ${meta.id}"
    
    input:
    val(meta)

    output:
    tuple val(meta), path("results.list"),  emit: list
    tuple val(meta), path("results.csv"), emit: csv
    path "versions.yml", emit: versions

    script:
    def args = task.ext.args ?: ''
    def conditions = task.ext.conditions ?: ''
    def metaQuery = meta.findAll { key, value -> key != 'id' }.collect { key, value -> "${key} = \"${value}\"" }.join(' and ')
    """
    # Find files/collections matching metadata query
    imeta qu $args ${metaQuery} $conditions | \
        grep -v "No rows found" | \
        sed -z -e 's/collection: //g' -e 's|\\ndataObj: |/|g' | \
        grep -v -- '---' > results.list
    
    # Create a csv list of files/collections found
    cat results.list | tr '\\n' ',' | sed 's/,\$//g' > results.csv

    if [ ! -s "results.list" ]; then
        echo "Error: No matching files/collections found for the $metaQuery"
        exit 3
    fi
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        irods: \$(ienv | grep version | awk '{ print \$3 }')
    END_VERSIONS
    """

    stub:
    def args = task.ext.args ?: '-z seq -d target = 1 and type != fastq and'
    def metaQuery = meta.findAll { key, value -> key != 'id' }.collect { key, value -> "${key} = \"${value}\"" }.join(' and ')
    """
    echo "some/path/to/file.ext" > results.list
    echo "some/path/to/collection" >> results.list

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        irods: \$(ienv | grep version | awk '{ print \$3 }')
    END_VERSIONS
    """
}