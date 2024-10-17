mport imaplib
import email
from email.header import decode_header
from openpyxl import Workbook
from datetime import datetime
import re
import os
import socket
import sys
import time

# IMAP server login credentials
EMAIL = "venkataramarajun9@gmail.com"
PASSWORD = "gorp kccb iwit mwwa"  # Consider using environment variables for security
IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993

# Updated paths for log and Excel files
LOG_FILE = "/usr/tmp/processed_dates_updated6.log"
EXCEL_FILE = "/usr/tmp/email_data6.xlsx"

# Timeout settings
socket.setdefaulttimeout(60)

# Batch size for fetching emails
BATCH_SIZE = 50

# Filter settings
SKIP_KEYWORDS = ["hotlist", "hot list", "available", "bench"]
SKIP_DOMAINS = ["google.com", "dice.com"]

# Handle errors by printing the message and exiting
def handle_error(message):
    print(f"Error occurred: {message}", file=sys.stderr)
    sys.exit(1)

# Load already processed timestamps from the log file
def load_processed_timestamps():
    try:
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'r') as file:
                return set(file.read().splitlines())
        return set()
    except Exception as e:
        handle_error(f"Failed to load processed timestamps: {e}")

# Save a processed timestamp to the log file
def save_processed_timestamp(timestamp_str):
    try:
        with open(LOG_FILE, 'a') as file:
            file.write(f"{timestamp_str}\n")
        print(f"Timestamp {timestamp_str} saved to log file.")
    except Exception as e:
        handle_error(f"Failed to save processed timestamp: {e}")

# Connect to IMAP server and fetch emails based on start and end date/time
def extract_emails_by_datetime_range(start_datetime, end_datetime, processed_timestamps):
    try:
        imap = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        imap.login(EMAIL, PASSWORD)
        imap.select("inbox")
    except Exception as e:
        handle_error(f"Failed to connect to IMAP server: {e}")

    query = f'SINCE "{start_datetime.strftime("%d-%b-%Y")}" BEFORE "{end_datetime.strftime("%d-%b-%Y")}"'

    try:
        result, data = imap.search(None, query)
        email_ids = data[0].split()
    except Exception as e:
        handle_error(f"Failed to search emails: {e}")

    emails = []
    total_emails = len(email_ids)
    print(f"Processing emails from {start_datetime} to {end_datetime}. Found {total_emails} emails.")

    for i in range(0, total_emails, BATCH_SIZE):
        batch = email_ids[i:i + BATCH_SIZE]
        batch_str = ",".join([email_id.decode('utf-8') for email_id in batch])

        attempt = 0
        max_attempts = 3
        while attempt < max_attempts:
            try:
                result, msg_data = imap.fetch(batch_str, "(RFC822)")
                for response_part in msg_data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_bytes(response_part[1])
                        email_data = extract_email_data(msg, processed_timestamps)
                        if email_data:
                            emails.append(email_data)
                print(f"Processed {min(i + BATCH_SIZE, total_emails)} of {total_emails} emails...")
                break
            except imaplib.IMAP4.abort as e:
                attempt += 1
                if attempt < max_attempts:
                    print(f"Retrying to fetch emails (Attempt {attempt}/{max_attempts}) due to error: {e}", file=sys.stderr)
                    time.sleep(5)
                else:
                    handle_error(f"Failed to fetch emails after {max_attempts} attempts: {e}")

    imap.logout()
    return emails

# Extract email data and apply filters
def extract_email_data(msg, processed_timestamps):
    try:
        received_date = email.utils.parsedate_to_datetime(msg.get("Date"))
        timestamp_str = received_date.strftime('%Y-%m-%d %H:%M:%S')

        if timestamp_str in processed_timestamps:
            return None

        subject = decode_header(msg["Subject"])[0][0]
        subject = subject.decode(errors='ignore') if isinstance(subject, bytes) else subject

        from_email = msg.get("From")
        reply_to = msg.get("Reply-To", from_email)
        email_source = reply_to or from_email

        if any(keyword in subject.lower() for keyword in SKIP_KEYWORDS):
            return None

        name, company, email_addr = extract_name_company_email(email_source)
        domain = extract_domain(email_addr)

        if any(skip_domain in domain for skip_domain in SKIP_DOMAINS):
            return None

        body = extract_body(msg)

        if any(keyword in body.lower() for keyword in SKIP_KEYWORDS):
            return None

        return {
            'received_date': received_date,
            'dateTime': timestamp_str,
            'name': name or "Unknown",
            'company': company or "Unknown",
            'email': email_addr,
            'subject': subject,
            'body': body.strip() if body else "No content",
        }
    except Exception as e:
        handle_error(f"Failed to extract email data: {e}")

# Extract name, company, and email from the source
def extract_name_company_email(source):
    try:
        match = re.match(r'^"?(.+?)"?\s*<(.+?)>$', source)
        if match:
            name_part, email_part = match.groups()
            name, company = (name_part.split(',', 1) if ',' in name_part else (name_part, "Unknown"))
            return name.strip(), company.strip(), email_part.strip()
        return "Unknown", "Unknown", source
    except Exception as e:
        handle_error(f"Failed to extract name/company/email: {e}")

# Extract the body of the email
def extract_body(msg):
    try:
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    return part.get_payload(decode=True).decode("utf-8", errors='ignore')
        else:
            return msg.get_payload(decode=True).decode("utf-8", errors='ignore')
    except Exception as e:
        handle_error(f"Failed to extract body: {e}")
    return ""

# Extract domain from email
def extract_domain(email):
    match = re.search(r'@([A-Za-z0-9.-]+)', email)
    return match.group(1) if match else ""

# Write the emails to Excel
def write_emails_to_excel(emails):
    try:
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "Emails"

        # Add headers
        sheet.append(["Received Date/Time", "Name", "Company", "Email", "Subject", "Body"])

        for email in emails:
            sheet.append([email['dateTime'], email['name'], email['company'], email['email'], email['subject'], email['body']])

        workbook.save(EXCEL_FILE)
        print(f"Emails saved to {EXCEL_FILE}")
    except Exception as e:
        handle_error(f"Failed to write emails to Excel: {e}")

# Main function to fetch emails based on a date/time range
def main(start_datetime_str, end_datetime_str):
    try:
        processed_timestamps = load_processed_timestamps()

        start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M:%S")

        all_emails = []

        print(f"Processing emails from {start_datetime} to {end_datetime}...")

        emails = extract_emails_by_datetime_range(start_datetime, end_datetime, processed_timestamps)

        if emails:
            all_emails.extend(emails)
            for email_data in emails:
                save_processed_timestamp(email_data['dateTime'])
        else:
            print("No emails found in this time range.")

        # Sort emails by received date
        all_emails.sort(key=lambda x: x['received_date'])

        if all_emails:
            write_emails_to_excel(all_emails)
        else:
            print("No emails found in the specified range.")

    except Exception as e:
        handle_error(f"Unexpected error: {e}")

if __name__ == '__main__':
    start_datetime_str = "2024-10-04 01:01:00"
    end_datetime_str = "2024-10-12 01:01:00"
    main(start_datetime_str, end_datetime_str)

