from azure.ai.textanalytics import (
        TextAnalyticsClient,
        AnalyzeSentimentAction,
        ExtractKeyPhrasesAction,
        ExtractiveSummaryAction
    )
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
import pypdf as PyPDF2
import os
from tokens import LANGUAGE_KEY, LANGUAGE_ENDPOINT, STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME


TEMP_PDF='temp.pdf'

def cleanning():
    os.remove(TEMP_PDF)


# Read the PDF files on the Stora Account
def pdf_read():
    text = ""

    # Storage Blob Client
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

    # Iterate over all of the blobs on container
    for blob in container_client.list_blobs():
        blob_name = blob.name
        if blob_name in ['resumao.pdf']:
            blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)

            # Download PDF from Blob Storage
            with open(TEMP_PDF, "ab") as download_file:
                download_file.write(blob_client.download_blob().readall())

            # Read and extract the text from PDF
            with open(TEMP_PDF, "rb") as file:
                reader = PyPDF2.PdfReader(file)
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


def document_analysis(client, text, print_text):
    documents = [text]

    poller = client.begin_analyze_actions(
        documents,
        display_name="Text Analysis",
        actions=[
            AnalyzeSentimentAction(),
            ExtractKeyPhrasesAction(),
            ExtractiveSummaryAction(max_sentence_count=4)
        ],
    )
    document_results = poller.result()

    for doc, action_results in zip(documents, document_results):
        if print_text:
            print(f"\nDocument text: {doc}")
        for result in action_results:
            if result.kind == "EntityRecognition":
                print("...Results of Recognize Entities Action:")
                for entity in result.entities:
                    print(f"......Entity: {entity.text}")
                    print(f".........Category: {entity.category}")
                    print(f".........Confidence Score: {entity.confidence_score}")
                    print(f".........Offset: {entity.offset}")

            elif result.kind == "PiiEntityRecognition":
                print("...Results of Recognize PII Entities action:")
                for pii_entity in result.entities:
                    print(f"......Entity: {pii_entity.text}")
                    print(f".........Category: {pii_entity.category}")
                    print(f".........Confidence Score: {pii_entity.confidence_score}")

            elif result.kind == "KeyPhraseExtraction":
                print("...Results of Extract Key Phrases action:")
                print(f"......Key Phrases: {result.key_phrases}")

            elif result.kind == "EntityLinking":
                print("...Results of Recognize Linked Entities action:")
                for linked_entity in result.entities:
                    print(f"......Entity name: {linked_entity.name}")
                    print(f".........Data source: {linked_entity.data_source}")
                    print(f".........Data source language: {linked_entity.language}")
                    print(
                        f".........Data source entity ID: {linked_entity.data_source_entity_id}"
                    )
                    print(f".........Data source URL: {linked_entity.url}")
                    print(".........Document matches:")
                    for match in linked_entity.matches:
                        print(f"............Match text: {match.text}")
                        print(f"............Confidence Score: {match.confidence_score}")
                        print(f"............Offset: {match.offset}")
                        print(f"............Length: {match.length}")

            elif result.kind == "SentimentAnalysis":
                print("...Results of Analyze Sentiment action:")
                print(f"......Overall sentiment: {result.sentiment}")
                print(
                    f"......Scores: positive={result.confidence_scores.positive}; \
                    neutral={result.confidence_scores.neutral}; \
                    negative={result.confidence_scores.negative} \n"
                )

            elif result.is_error is True:
                print(
                    f"...Is an error with code '{result.error.code}' and message '{result.error.message}'"
                )

        print("------------------------------------------")



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

        result = client.extract_key_phrases(documents)
        for idx, doc in enumerate(result):
            if not doc.is_error:
                print("Key phrases in article #{}: {}".format(
                    idx + 1,
                    ", ".join(doc.key_phrases)
                ))


if __name__ == '__main__':
    client = authenticate_client()
    text = pdf_read()
    #sentiment_analysis_with_opinion_mining(client, text, detaild_report=False)
    document_analysis(client, text, print_text=False)
    cleanning()