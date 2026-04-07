def get_anonymization_recommendations(self, table_name, columns, sample_data_json):
    """
    Šalje imena kolona + 5 redova uzorka podataka Azure AI-ju.
    """

    # Prompt koji koristi tvoj novi Enterprise argument o privatnosti
    system_prompt = (
        "You are an expert Data Privacy Officer. Your task is to analyze database columns "
        "and their content to recommend anonymization strategies. "
        "Strictly follow the Azure OpenAI Enterprise privacy guidelines: data provided is for "
        "contextual analysis only and is not used for model training."
    )

    user_prompt = f"""
    Analyze the table: '{table_name}'

    Columns provided: {columns}

    Sample Data (first 5 rows in JSON):
    {sample_data_json}

    For each column, choose the best strategy:
    - 'faker_name': for human names
    - 'faker_email': for email addresses
    - 'faker_phone': for phone numbers
    - 'hash': for unique identifiers (IDs, usernames)
    - 'mask': for sensitive strings or numbers that aren't unique IDs
    - 'keep': for non-sensitive data (dates, amounts, categories)

    Return the result ONLY as a valid JSON object: {{"column_name": "strategy"}}
    """

    # Ovde ide tvoj postojeći poziv ka Azure OpenAI
    # response = self.client.chat.completions.create(
    #    model="gpt-4o", # ili tvoj deployment name
    #    messages=[
    #        {"role": "system", "content": system_prompt},
    #        {"role": "user", "content": user_prompt}
    #    ],
    #    response_format={ "type": "json_object" }
    # )