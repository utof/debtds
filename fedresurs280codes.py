import json

def process_inn_json(input_path, output_path):
    """Process a JSON file with INN entries and assign codes based on conditions."""
    # Load the input JSON file
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Initialize output dictionary
    output_data = {}

    # Process each INN entry
    for inn, entry in data.items():
        code = 1  # Default code

        # Check for "Информация не найдена" (code 0)
        if 'message' in entry and entry['message'] == "Информация не найдена":
            code = 0
        else:
            # Check 'rez' array for description or status
            if 'rez' in entry and len(entry['rez']) > 0:
                record = entry['rez'][0]
                if 'description' in record:
                    description = record['description'].get('value', '')
                    if 'Конкурсное производство' in description:
                        code = 3
                    elif description == "Наблюдение":
                        code = 2
                    elif description == "Производство по делу прекращено":
                        code = 4
                if 'status' in record and record['status'].get('value', '') == "Производство по делу прекращено":
                    code = 4

        # Add the assigned code to output
        output_data[inn] = code

    # Save the output JSON file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    # Process debtor JSON
    INPUT_PATH_DEBTOR = 'debtor_responses.json'
    OUTPUT_PATH_DEBTOR = 'debtor_codes.json'
    process_inn_json(INPUT_PATH_DEBTOR, OUTPUT_PATH_DEBTOR)

    # Process creditor JSON
    INPUT_PATH_CREDITOR = 'creditor_responses.json'
    OUTPUT_PATH_CREDITOR = 'creditor_codes.json'
    process_inn_json(INPUT_PATH_CREDITOR, OUTPUT_PATH_CREDITOR)

