import os
from typing import List, Dict
from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate  # Updated path
from langchain_core.output_parsers import PydanticOutputParser # Updated path

# 1. Define the structure for the AI response
class AnonymizationPlan(BaseModel):
    column: str = Field(description="Name of the database column")
    strategy: str = Field(description="Anonymization strategy: 'hash', 'mask', 'noise', or 'none'")
    reason: str = Field(description="Brief explanation in English why this strategy was chosen")

class PrivacyAnalysis(BaseModel):
    plan: List[AnonymizationPlan]

# 2. Privacy Agent Class
class PrivacyAgent:
    def __init__(self):
        # host.docker.internal allows Docker container to see Ollama on the host machine
        self.llm = ChatOllama(
            model="llama3",
            base_url="http://host.docker.internal:11434",
            temperature=0
        )
        self.parser = PydanticOutputParser(pydantic_object=PrivacyAnalysis)

    def analyze_metadata(self, metadata_package: List[Dict]) -> PrivacyAnalysis:
        """
        Analyses the shuffled and masked metadata using Llama 3.
        """
        template = """
        You are a Senior Data Privacy Expert (GDPR & HIPAA Compliance).
        Analyze the following database metadata and sample values.

        CRITICAL RULES:
        1. Identify ALL columns containing PII (Personally Identifiable Information).
        2. Columns like 'phone_number', 'email', 'salary', 'first_name', 'last_name', and 'birth_date' MUST be flagged.
        3. Even if the sample is partially masked, you must identify the data category.
        4. For phone numbers, use the 'mask' strategy.
        5. For names and emails, use 'hash' or 'mask'.
        6. For financial data like salary, use 'noise'.
        7. If no PII is found, set strategy to 'none'.

        Metadata Package:
        {metadata}

        {format_instructions}

        IMPORTANT: Return ONLY valid JSON that matches the format instructions. Do not include any conversational text before or after the JSON.
        """

        prompt = ChatPromptTemplate.from_template(template)

        # Prepare instructions and data
        format_instructions = self.parser.get_format_instructions()
        formatted_prompt = prompt.format(
            metadata=metadata_package,
            format_instructions=format_instructions
        )

        try:
            response = self.llm.invoke(formatted_prompt)
            # Parse the response into Pydantic object
            return self.parser.parse(response.content)
        except Exception as e:
            print(f"Error calling Llama 3: {e}")
            return None