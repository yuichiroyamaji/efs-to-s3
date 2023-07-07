from datetime import datetime, timedelta, timezone
import json
import os
import subprocess
import boto3
import logging

def lambda_handler(event, context):
    
    # ログ設定
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    logger.info('#### START FUNCTION ####')
    ec2_zip_path = os.environ['EC2_ZIP_PATH']
    try:
        
        # S3設定
        s3_client = boto3.client('s3')
        destination_buket = os.environ['S3_BUCKET']
        destination_folder = os.environ['S3_FOLDER']
        
        # EFSのマウントパスは以下のディレクトリをリスト
        logger.info('#### ROOT PATH FOLDERS ####')
        root_path = os.environ['EFS_ROOT_PATH']
        zip_path = os.environ['EFS_ZIP_PATH']
        files = os.listdir(root_path)
        logger.info(files)
        
        # 2週間前の日時取得
        logger.info('#### TARGET DATE(2 WEEKS AGO) ####')
        current_time = datetime.now()
        jst_current_time = current_time + timedelta(hours=9)
        two_weeks_ago = jst_current_time - timedelta(weeks=2)
        logger.info(two_weeks_ago)
        
        # EFS側のZIPログファイルをループで処理
        logger.info('#### TARGET FILES ####')
        logs = os.listdir(zip_path)
        target_files = ""
        cnt = 0
        for count, log in enumerate(logs):
            target_file = zip_path + '/' + log
            last_modified_timestamp = os.path.getmtime(target_file)
            last_modified_date = datetime.fromtimestamp(last_modified_timestamp)
            # 対象のファイルの最終更新日時が2週間以上前であればS3に移動
            if log != 'zip' and last_modified_date < two_weeks_ago:
                cnt +=  1 
                # EFSファイルの読み込み
                key_name = destination_folder + log
                with open(target_file, 'rb') as file:
                    file_contents = file.read()
                # S3へfileをupload
                s3_client.put_object(
                    Bucket=destination_buket,
                    Key=key_name,
                    Body=file_contents
                )
                # EFSファイルの削除
                os.remove(target_file)
                target_files += log + '（最終更新日:' + str(last_modified_date) + '）\r\n'
                # 移行ファイル数を制限する場合は以下コメントを解除
                #if cnt >= 500:
                #    break
        
        statusCode = 200
        message = 'EFS上の [' + str(two_weeks_ago) + '] 以前のファイルがS3に移行されました。\r\n\r\n'\
              '移行元EFSフォルダ名：' + ec2_zip_path + '\r\n\r\n'\
              '移行先S3バケット名：' + destination_buket + '\r\n\r\n'\
              '移行完了ログファイル：' + '\r\n\r\n' + target_files
            
    except Exception as e:
        logger.info("ExceptionArgs:")
        logger.info(e.args)
        statusCode = 500
        message = '想定外のエラーが発生しました。ログを確認してください。'
    
    # SNS件名設定
    subject = '【EFSログS3移行通知】 ' + os.environ['ENV_NAME'] + ' ' + str(cnt) + 'ファイル移行完了'
    
    # SNSオブジェクト取得
    sns_client = boto3.client('sns')
    
    # 環境変数よりSNSトピックARN取得
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    
    # SNS送信
    logger.info('#### SNS SUBJECT ####')
    logger.info(subject)
    logger.info('#### SNS MESSAGE ####')
    logger.info(message)
    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=message
    )
    logger.info('#### SNS RESPONSE ####')
    logger.info(response)

    logger.info('#### END FUNCTION ####')
    return {
        'statusCode': statusCode,
        'message': json.dumps(message)
    }
