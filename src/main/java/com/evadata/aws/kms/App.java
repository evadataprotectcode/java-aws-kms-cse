package com.evadata.aws.kms;

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoInputStream;
import com.amazonaws.encryptionsdk.CryptoOutputStream;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import org.apache.commons.cli.*;

import java.io.*;

public class App {
    public static void main( String[] args ){
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();

        Option keyArnOption = Option.builder("k").longOpt("key-arn").hasArg().desc("KMS key arn to use").required().build();
        Option s3BucketOption = Option.builder("s").longOpt("s3-bucket-name").hasArg().desc("S3 bucket name").required().build();
        Option filePathOption = Option.builder("f").longOpt("file").hasArg().desc("Path to file that for upload/download").required().build();
        Option s3KeyOption = Option. builder("o").longOpt("s3-key").hasArg().desc("S3 Object Key").required().build();

        Option uploadOption = Option.builder("u").longOpt("upload").desc("Encrypt and upload the file to the specified S3 bucket").build();
        Option downloadOption = Option.builder("d").longOpt("download").desc("Download and decrypt the file to the specified S3 bucket").build();

        OptionGroup uploadOrDownloadOptionGroup = new OptionGroup();
        uploadOrDownloadOptionGroup.addOption(uploadOption);
        uploadOrDownloadOptionGroup.addOption(downloadOption);
        uploadOrDownloadOptionGroup.setRequired(true);

        options.addOption(keyArnOption);
        options.addOption(s3BucketOption);
        options.addOption(filePathOption);
        options.addOption(s3KeyOption);

        options.addOptionGroup(uploadOrDownloadOptionGroup);

        try {
            CommandLine line = parser.parse(options, args);

            String keyArn = line.getOptionValue("key-arn");
            String bucketName = line.getOptionValue("s3-bucket-name");
            String path = line.getOptionValue("file");
            String s3Key = line.getOptionValue("s3-key");

            File file = new File(path);

            if(line.hasOption("u")) {
                upload(keyArn, bucketName, s3Key, file);
                System.exit(0);
            }

            download(keyArn, bucketName, s3Key, file);

        } catch(MissingArgumentException missingArgumentEx) {
            System.out.println(missingArgumentEx.getMessage());
            printHelp(options);

        } catch(MissingOptionException missingOptionEx) {
            System.out.println(missingOptionEx.getMessage());
            printHelp(options);

        } catch(ParseException exp) {
            System.out.println( "Unexpected exception:" + exp.getMessage() );
        }

        System.exit(0);
    }

    private static void upload(String keyArn, String bucketName, String s3Key, File file) {
        try {
            FileInputStream inputStream = new FileInputStream(file);
            FileOutputStream outStream = new FileOutputStream(file.getName()+".encrypted");

            final AwsCrypto awsCrypto = AwsCrypto.builder()
                    .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
                    .build();
            final KmsMasterKeyProvider keyProvider = KmsMasterKeyProvider.builder().buildStrict(keyArn);

            CryptoOutputStream<?> encryptingStream = awsCrypto.createEncryptingStream(keyProvider, outStream);

            IOUtils.copy(inputStream, encryptingStream);

            encryptingStream.flush();
            encryptingStream.close();

            File targetFile = new File(file.getName()+".encrypted");

            AmazonS3 s3Client = AmazonS3Client.builder().build();
            s3Client.putObject(bucketName, s3Key, targetFile);

            targetFile.deleteOnExit();

        } catch(FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void download(String keyArn, String bucketName, String s3Key, File file) {
        try {
            FileOutputStream outputStream = new FileOutputStream(file);

            final AwsCrypto awsCrypto = AwsCrypto.builder()
                    .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
                    .build();
            final KmsMasterKeyProvider keyProvider = KmsMasterKeyProvider.builder().buildStrict(keyArn);

            AmazonS3 s3Client = AmazonS3Client.builder().build();
            S3Object s3Object = s3Client.getObject(bucketName, s3Key);

            InputStream s3Stream = s3Object.getObjectContent();

            CryptoInputStream<?> decryptingStream = awsCrypto.createDecryptingStream(keyProvider, s3Stream);

            IOUtils.copy(decryptingStream, outputStream);

            outputStream.flush();
            outputStream.close();

        }catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("App", options);
    };
}
