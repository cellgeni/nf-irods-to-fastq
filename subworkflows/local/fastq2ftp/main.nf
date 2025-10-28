include { CONCATENATE_FASTQS } from '../../../modules/local/concatenate_fastqs'
include { UPLOAD2FTP } from '../../../modules/local/upload2ftp'
include { CALCULATE_MD5 } from '../../../modules/local/calculate_md5'

workflow FASTQS2FTP {
    take:
    fastqs // channel [val(meta), path(fastqs)]
    username
    password
    ftp_host
    ftp_path

    main:
    // STEP 1: Concatenate fastq files per sample
    CONCATENATE_FASTQS(fastqs)

    // Flatten the channel to work with individual files
    concat_fastqs = CONCATENATE_FASTQS.out.fastq.transpose()

    // STEP 2: Calculate md5 for each fastq file
    CALCULATE_MD5(concat_fastqs)

    // STEP 3: Upload fastq files to FTP server
    UPLOAD2FTP(
        concat_fastqs,
        username,
        password,
        ftp_host,
        ftp_path
    )

    // STEP 4: Collect versions
    versions = UPLOAD2FTP.out.versions.first()
        .mix(CALCULATE_MD5.out.versions.first())

    emit:
        versions = versions // channel of versions.yml files
        fastqs = CALCULATE_MD5.out.md5 // channel of [var(meta), fastq, md5]
}