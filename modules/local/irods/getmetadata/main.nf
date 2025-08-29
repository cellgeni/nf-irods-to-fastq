process IRODS_GETMETADATA {
    tag "Collecting metadata for $irodspath"

    input:
    tuple val(meta), val(irodspath)

    output:
    tuple val(meta), val(irodspath), path("metadata.tsv"), emit: tsv
    path "versions.yml"           , emit: versions

    script:
    irodspath = irodspath.replaceFirst('/$', '')
    """
    # Check if irodspath exists
    name=\$(basename "$irodspath")
    coll=\$(dirname "$irodspath")
    if iquest --no-page "SELECT COLL_ID WHERE COLL_NAME = '$irodspath'" | grep -q 'COLL_ID'; then
        resource="-C"
    elif iquest --no-page "SELECT DATA_ID WHERE COLL_NAME = '\$coll' AND DATA_NAME = '\$name'" | grep -q 'DATA_ID'; then
        resource="-d"
    else
        echo "Error: iRODS path $irodspath does not exist."
        exit 1
    fi

    # Get metadata from iRODS
    get_metadata.sh \$resource "$irodspath" > metadata.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        irods: \$(ienv | grep version | awk '{ print \$3 }')
    END_VERSIONS
    """

    stub:
    """
    touch metadata.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        irods: \$(ienv | grep version | awk '{ print \$3 }')
    END_VERSIONS
    """
}