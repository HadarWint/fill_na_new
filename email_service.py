import boto3
from botocore.exceptions import ClientError
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import email.mime.application
import os


class EMailService:
    def __init__(self, sender: str, recipients: str, subject: str,  date):
        aws_profile = os.getenv('PEPSICO_PREDICTOR_SVC_AWS_PROFILE')
        boto3.setup_default_session(profile_name=aws_profile)
        self.client = boto3.client('ses')
        self.status = 'init'
        self.sender = sender
        self.recipients = recipients.split(',')
        self.subject = subject
        self.aws_region = "eu-west-1"
        self.date = date

    def adjust_text(self):
        return f"please find attached a csv entailing the before and after null completion of each manufacturing " \
               f"line\n in the pervious day, the {self.date} "

    def send_email(self, content: str, data):
        """
        param content: the content that will appear in the body of the email
        param data: the df summarizing output of fill_na completions
        return: an email to each one of the recipients (defined in the deploy_config)
        """

        for recipient in self.recipients:
            subject = self.subject
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.sender
            msg['To'] = recipient

            body_html = f'<html> <head></head>' \
                        f'<body>' \
                        f'{content}' \
                        f'</body> </html>'

            body = MIMEText(body_html, 'html', 'utf-8')
            msg.attach(body)
            att = email.mime.application.MIMEApplication(data.to_csv())
            att.add_header('Content-Disposition', 'attachment; filename=' + f'{self.date}.csv')
            msg.attach(att)
            # Try to send the email.
            try:
                response = self.client.send_raw_email(
                    Source=self.sender,
                    Destinations=[recipient],
                    RawMessage={'Data': msg.as_string()}
                )
            # Display an error if something goes wrong.
            except ClientError as e:
                print(e.response['Error']['Message'])
                self.status = e
            else:
                print("Email sent! Message ID:"),
                print(response['MessageId'])
                self.status = '200'
