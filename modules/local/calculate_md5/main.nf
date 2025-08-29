process CALCULATE_MD5 {
    tag "Calculating MD5 checksums for ${file}"
    
    input:
    tuple val(meta), path(file)

    output:
    tuple val(meta), path(file), env('md5'), emit: md5
    path "versions.yml", emit: versions

    script:
    """
    md5=\$(md5sum ${file} | awk '{ print \$1 }')

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        md5sum: \$(md5sum --version | head -n 1 | awk '{ print \$4 }')
    END_VERSIONS
    """

    stub:
    """
    touch md5checksums.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        md5sum: \$(md5sum --version | head -n 1 | awk '{ print \$4 }')
    END_VERSIONS
    """
}