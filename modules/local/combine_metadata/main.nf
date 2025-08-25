process COMBINE_METADATA {
    tag "Combining metadata"
    container 'docker://quay.io/cellgeni/toh5ad:latest'

    input:
    val(metalist)

    output:
    path "metadata.csv", emit: csv
    path "metadata.log", emit: log
    path "versions.yml", emit: versions
    
    script:
    // Create JSON from the entire metalist (array of objects)
    def json = new groovy.json.JsonBuilder(metalist)
    new File("metadata.json").text = json.toPrettyString()
    """
    validate_metadata.py \\
        metadata.json
        --sample_column sample \\
        --cram_column cram_path \\
        --prefix_column fastq_prefix \\
        --check_duplicated_prefix \\
        --check_readcounts \\
        --check_library_types \\
        --check_readlengths \\
        --logfile metadata.log \\
        --output metadata.csv \\
        --sep , \\
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version | awk '{ print \$2 }')
        pandas: \$(python -c "import pandas; print(pandas.__version__)")
    END_VERSIONS
    """

    stub:
    """
    touch metadata.csv
    touch metadata.log
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version | awk '{ print \$2 }')
        pandas: \$(python -c "import pandas; print(pandas.__version__)")
    END_VERSIONS
    """
}