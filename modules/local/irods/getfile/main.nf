process IRODS_GETFILE {
    tag "Downloading $irodspath"

    input:
    tuple val(meta), val(irodspath)

    output:
    tuple val(meta), path("$filename"), emit: file
    path 'versions.yml', emit: versions

    script:
    def args = task.ext.args ?: ''
    filename = irodspath.tokenize('/').last()
    """
    # Download file from iRODS
    iget $args $irodspath $filename

    # Verify md5 checksum
    md5=\$(md5sum "$filename" | awk '{print \$1}')
    irods_md5=\$(ichksum "$irodspath" | awk '{print \$NF}')
    if [ "\$md5" != "\$irods_md5" ]; then
        echo "MD5 mismatch for $filename: local \$md5, iRODS \$irods_md5"
        exit 1
    fi

    # Create versions file
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        irods: \$(ienv | grep version | awk '{ print \$3 }')
    END_VERSIONS
    """

    stub:
    filename = irodspath.tokenize('/').last()
    """
    touch $filename

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        irods: \$(ienv | grep version | awk '{ print \$3 }')
    END_VERSIONS
    """
}