process UPLOAD2FTP {
    tag "Uploading FASTQ files for sample ${meta.id}"
    
    input:
    tuple val(meta), path(fastqs)
    val username
    val password
    val ftp_host
    val ftp_path

    output:
    path "versions.yml", emit: versions
    
    script:
    """
    lftp -u ${username},${password} ${ftp_host} -e "set ftp:ssl-allow no; cd ${ftp_path}; mput ${fastqs}; bye"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        lftp: \$(lftp --version | head -n 1 | awk '{ print \$4 }')
    END_VERSIONS
    """

    stub:
    """
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        lftp: \$(lftp --version | head -n 1 | awk '{ print \$4 }')
    END_VERSIONS
    """
}