process COMBINE_METADATA {
    tag "Combining metadata"
    container 'docker://quay.io/cellgeni/toh5ad:latest'

    input:
    val(metalist)

    output:
    path "*.csv",         emit: csv, optional: true
    path "*.tsv",         emit: tsv, optional: true
    path "metadata.json", emit: json
    path "warnings.log",  emit: log
    path "versions.yml",  emit: versions
    
    script:
    // Create JSON from the entire metalist (array of objects)
    json = new groovy.json.JsonBuilder(metalist)
    args = task.ext.args ? task.ext.args : ''
    template 'collect_metadata.sh'

    stub:
    """
    touch metadata.csv
    touch warnings.log
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version | awk '{ print \$2 }')
        pandas: \$(python -c "import pandas; print(pandas.__version__)")
    END_VERSIONS
    """
}