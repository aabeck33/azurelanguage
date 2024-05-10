from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
import PyPDF2
from tokens import LANGUAGE_KEY, LANGUAGE_ENDPOINT, STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME


# Read the PDF files on the Stora Account
def pdf_read():
    # Storage Blob Client
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

    # Iterate over all of the blobs on container
    for blob in container_client.list_blobs():
        blob_name = blob.name
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)

        # Download PDF from Blob Storage
        with open("temp.pdf", "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        # Read and extract the text from PDF
        with open("temp.pdf", "rb") as file:
            reader = PyPDF2.PdfReader(file)
            text = ""
            for page in range(len(reader.pages)):
                text += reader.pages[page].extract_text()
    return text


# Authenticate the client using your key and endpoint
def authenticate_client():
    ta_credential = AzureKeyCredential(LANGUAGE_KEY)
    text_analytics_client = TextAnalyticsClient(
            endpoint=LANGUAGE_ENDPOINT, 
            credential=ta_credential)
    return text_analytics_client

# Detecting sentiment and opinions in text
def sentiment_analysis_with_opinion_mining(client, text, detaild_report):

    documents = [text]

    result = client.analyze_sentiment(documents, show_opinion_mining=True)
    doc_result = [doc for doc in result if not doc.is_error]

    positive_reviews = [doc for doc in doc_result if doc.sentiment == "positive"]
    negative_reviews = [doc for doc in doc_result if doc.sentiment == "negative"]

    positive_mined_opinions = []
    mixed_mined_opinions = []
    negative_mined_opinions = []

    for document in doc_result:
        print("Document Sentiment: {}".format(document.sentiment))
        print("Overall scores: positive={0:.2f}; neutral={1:.2f}; negative={2:.2f} \n".format(
            document.confidence_scores.positive,
            document.confidence_scores.neutral,
            document.confidence_scores.negative,
        ))
        if detaild_report:
            for sentence in document.sentences:
                print("Sentence: {}".format(sentence.text))
                print("Sentence sentiment: {}".format(sentence.sentiment))
                print("Sentence score:\nPositive={0:.2f}\nNeutral={1:.2f}\nNegative={2:.2f}\n".format(
                    sentence.confidence_scores.positive,
                    sentence.confidence_scores.neutral,
                    sentence.confidence_scores.negative,
                ))
                for mined_opinion in sentence.mined_opinions:
                    target = mined_opinion.target
                    print("......'{}' target '{}'".format(target.sentiment, target.text))
                    print("......Target score:\n......Positive={0:.2f}\n......Negative={1:.2f}\n".format(
                        target.confidence_scores.positive,
                        target.confidence_scores.negative,
                    ))
                    for assessment in mined_opinion.assessments:
                        print("......'{}' assessment '{}'".format(assessment.sentiment, assessment.text))
                        print("......Assessment score:\n......Positive={0:.2f}\n......Negative={1:.2f}\n".format(
                            assessment.confidence_scores.positive,
                            assessment.confidence_scores.negative,
                        ))
                print("\n")
        print("\n")


if __name__ == '__main__':
    client = authenticate_client()
    text = pdf_read()
    sentiment_analysis_with_opinion_mining(client, text, detaild_report=False)