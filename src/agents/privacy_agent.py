# -*- coding: utf-8 -*-
import os
from typing import List, Dict
from pydantic import BaseModel, Field
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser

# 1. Osnovna struktura za jednu kolonu
class AnonymizationPlan(BaseModel):
    column: str = Field(description="Name of the database column")
    is_pii: bool = Field(default=False, description="True if sensitive")
    strategy: str = Field(description="Strategy: 'hash', 'mask', 'faker_name', 'faker_email', 'faker_phone', or 'keep'")
    reason: str = Field(description="Brief explanation in English")

# 2. Omotač koji sadrži listu planova (Ovo je nedostajalo!)
class PrivacyAnalysis(BaseModel):
    plan: List[AnonymizationPlan]

# 3. Glavna klasa Agent
class PrivacyAgent:
    def __init__(self):
        # Inicijalizacija Azure klijenta
        self.llm = AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_version=os.getenv("AZURE_OPENAI_VERSION"),
            temperature=0
        )
        self.parser = PydanticOutputParser(pydantic_object=PrivacyAnalysis)

    def analyze_metadata(self, metadata_package: List[Dict]) -> PrivacyAnalysis:
        """
        Prima listu rečnika: [{'column': 'email', 'sample': ['a@b.com', ...]}, ...]
        Šalje uzorak podataka Azure AI-ju za preciznu analizu.
        """
        template = """
        You are a Senior Data Privacy Expert (GDPR & HIPAA Compliance).

        ENTERPRISE PRIVACY POLICY: 
        The sample data provided is for contextual analysis only. 
        It MUST NOT be used for model training or stored beyond this session.

        TASK:
        Analyze the database columns and their actual sample values to identify PII (Personally Identifiable Information).
        Use the provided sample values to confirm the context, as column names can be misleading.

        COLUMNS AND SAMPLES:
        {metadata}

        {format_instructions}

        STRATEGY GUIDE:
        - 'faker_name': Use for full names, first names, or last names.
        - 'faker_email': Use for any email addresses.
        - 'faker_phone': Use for phone numbers (recognize formats from samples).
        - 'hash': Use for unique IDs (UUIDs, UserIDs) that must remain consistent.
        - 'mask': Use for sensitive strings like addresses or descriptions.
        - 'keep': Use for safe, non-identifiable data (counts, timestamps, amounts, statuses).

        IMPORTANT: Return ONLY valid JSON.
        """

        prompt = ChatPromptTemplate.from_template(template)
        format_instructions = self.parser.get_format_instructions()
        
        # LangChain Chain
        chain = prompt | self.llm | self.parser
        
        try:
            # Slanje paketa sa uzorcima podataka ka Azure AI
            return chain.invoke({
                "metadata": metadata_package,
                "format_instructions": format_instructions
            })
        except Exception as e:
            print(f"❌ Azure Analysis Error: {e}")
            return None