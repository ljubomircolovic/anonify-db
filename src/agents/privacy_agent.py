# -*- coding: utf-8 -*-
import os
from typing import List, Dict
from pydantic import BaseModel, Field
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser

# 1. Struktura za validaciju odgovora (Pydantic)
class AnonymizationPlan(BaseModel):
    column: str = Field(description="Name of the database column")
    is_pii: bool = Field(default=False, description="True if sensitive")
    strategy: str = Field(description="Strategy: 'hash', 'mask', 'noise', 'synthetic', or 'keep'")
    reason: str = Field(description="Brief explanation in English")

class PrivacyAnalysis(BaseModel):
    plan: List[AnonymizationPlan]

# 2. Glavna klasa koja sada koristi Azure OpenAI
class PrivacyAgent:
    def __init__(self):
        # Inicijalizacija Azure klijenta koristeći env varijable
        self.llm = AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_version=os.getenv("AZURE_OPENAI_VERSION"),
            temperature=0
        )
        self.parser = PydanticOutputParser(pydantic_object=PrivacyAnalysis)

    # U src/ai/privacy_agent.py

# U src/ai/privacy_agent.py

    def analyze_metadata(self, metadata_package: List[Dict]) -> PrivacyAnalysis:
        """
        Prima listu rečnika (kolona + semplovi) i vraća JEDAN PrivacyAnalysis objekat.
        """
        template = """
        You are a Senior Data Privacy Expert (GDPR & HIPAA Compliance).
        Analyze the following list of database columns and their sample values.
        
        COLUMNS TO ANALYZE:
        {metadata}

        {format_instructions}

        CORE RULES:
        1. Identify ALL columns containing PII.
        2. Suggest a strategy: 'hash', 'mask', 'synthetic', 'noise', or 'keep'.
        3. If NO PII is found, set strategy to 'keep' and is_pii to false.
        
        IMPORTANT: Return ONLY valid JSON.
        """

        prompt = ChatPromptTemplate.from_template(template)
        format_instructions = self.parser.get_format_instructions()
        
        # Moderni LangChain Pipe workflow
        chain = prompt | self.llm | self.parser
        
        try:
            # JEDAN POZIV KA AZURE-U ZA SVE KOLONE ODJEDNOM
            return chain.invoke({
                "metadata": metadata_package,
                "format_instructions": format_instructions
            })
        except Exception as e:
            print(f"❌ Azure Batch Error: {e}")
            return None