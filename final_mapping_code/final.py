import pandas as pd
import os

def map_csv_to_csv(source_csv, target_csv, output_folder):
    """Map values from source CSV to target CSV while preserving static values."""
    try:
        # Read CSV with proper encoding handling
        source_df = pd.read_csv(source_csv, encoding="ISO-8859-1", on_bad_lines="skip", engine="python")
        target_df = pd.read_csv(target_csv, encoding="ISO-8859-1", on_bad_lines="skip", engine="python")

        print("\n[INFO] Source CSV Columns:", source_df.columns.tolist())
        print("\n[INFO] Target CSV Columns:", target_df.columns.tolist())

    except Exception as e:
        print(f"[ERROR] Failed to read files: {e}")
        return
    
    if "JSON Schema 5.0 Data Field" not in target_df.columns:
        raise ValueError("[ERROR] Target CSV must contain a 'JSON Schema 5.0 Data Field' column")

    os.makedirs(output_folder, exist_ok=True)

    # ✅ Extract initials from givenName
    if "awardeeDetail/affiliationOf/givenName" in source_df.columns:
        source_df["awardeeDetail/affiliationOf/initials"] = source_df["awardeeDetail/affiliationOf/givenName"].astype(str).str[0].str.upper() + "."
        source_df["awardeeDetail/affiliationOf/initials"] = source_df["awardeeDetail/affiliationOf/initials"].where(source_df["awardeeDetail/affiliationOf/givenName"].notna(), "")
    else:
        print("[ERROR] Given name column is missing in the Source CSV!")
        return

    # ✅ Extract fundingBodyAwardId properly
    if "fundingBodyAwardId" in source_df.columns:
        source_df["fundingBodyAwardId"] = source_df["fundingBodyAwardId"].astype(str).str.strip()
        source_df["fundingBodyAwardId"] = source_df["fundingBodyAwardId"].fillna("NOT FOUND")
    else:
        print("[ERROR] 'fundingBodyAwardId' column is missing in the Source CSV!")
        return


    # ✅ Merge givenName and familyName into a full name
    if "awardeeDetail/affiliationOf/givenName" in source_df.columns and "awardeeDetail/affiliationOf/familyName" in source_df.columns:
        source_df["awardeeDetail/affiliationOf/name"] = (
            source_df["awardeeDetail/affiliationOf/givenName"].fillna('') + " " + 
            source_df["awardeeDetail/affiliationOf/familyName"].fillna('')
        ).str.strip()

    # ✅ Ensure `createdOn` and `endDate` exist before processing
    if "endDate" in source_df.columns and "createdOn" in source_df.columns:
        source_df["endDate"] = pd.to_datetime(source_df["endDate"], errors="coerce", dayfirst=True).dt.date
        source_df["createdOn"] = pd.to_datetime(source_df["createdOn"], errors="coerce", dayfirst=True).dt.date
    else:
        print("[ERROR] 'endDate' or 'createdOn' column is missing in the Source CSV!")
        return  # Stop execution if these columns are missing

    # ✅ Convert funding amount to whole number
    if "fundingDetail/fundingTotal/amount" in source_df.columns:
        source_df["fundingDetail/fundingTotal/amount"] = source_df["fundingDetail/fundingTotal/amount"].astype(str).str.split('.').str[0]
        source_df["fundingDetail/fundingTotal/amount"] = source_df["fundingDetail/fundingTotal/amount"].fillna("NOT FOUND")
    else:
        print("[ERROR] 'fundingDetail/fundingTotal/amount' column is missing in the Source CSV!")
        return

    # ✅ Fill missing department names with "NOT FOUND"
    if "awardeeDetail/departmentName" in source_df.columns:
        source_df["awardeeDetail/departmentName"] = source_df["awardeeDetail/departmentName"].fillna("NOT FOUND")
    else:
        print("[ERROR] 'awardeeDetail/departmentName' column is missing in the Source CSV!")
        return

    # ✅ Fill missing ORCID identifiers with "NOT FOUND"
    if "awardeeDetail/affiliationOf/identifier/id/orcid" in source_df.columns:
        source_df["awardeeDetail/affiliationOf/identifier/id/orcid"] = source_df["awardeeDetail/affiliationOf/identifier/id/orcid"].fillna("NOT FOUND")
    else:
        print("[ERROR] 'awardeeDetail/affiliationOf/identifier/id/orcid' column is missing in the Source CSV!")
        return

    # Process each row efficiently
    for index, row in source_df.iterrows():
        source_mapping = row.to_dict()
        updated_target_df = target_df.copy()

        def update_value(field_name, current_value):
            return current_value if pd.notna(current_value) and current_value != "" else source_mapping.get(field_name, "NOT FOUND")

        # ✅ Add fundingBodyAwardId mapping (Ensure extraction works for values like 11/0004192)
        updated_target_df.loc[updated_target_df["JSON Schema 5.0 Data Field"] == "fundingBodyAwardId","Value from the Source or as determined by Supplier"] = row["fundingBodyAwardId"]
    
        updated_target_df["Value from the Source or as determined by Supplier"] = updated_target_df.apply(
            lambda row: update_value(row["JSON Schema 5.0 Data Field"], row["Value from the Source or as determined by Supplier"]),
            axis=1
        )

        # ✅ Handle funds/status only if both dates exist
        if pd.notna(row.get("endDate")) and pd.notna(row.get("createdOn")):
            funds_status = "OPEN" if row["endDate"] > row["createdOn"] else "CLOSED"
        else:
            funds_status = "UNKNOWN"  # Handle missing values

        # Update funds/status in the target DataFrame
        updated_target_df.loc[updated_target_df["JSON Schema 5.0 Data Field"] == "funds/status", "Value from the Source or as determined by Supplier"] = funds_status

        # ✅ Add initials mapping
        updated_target_df.loc[updated_target_df["JSON Schema 5.0 Data Field"] == "awardeeDetail/affiliationOf/initials", "Value from the Source or as determined by Supplier"] = row["awardeeDetail/affiliationOf/initials"]

        # ✅ Add funding amount (whole number only)
        updated_target_df.loc[updated_target_df["JSON Schema 5.0 Data Field"] == "fundingDetail/fundingTotal/amount", "Value from the Source or as determined by Supplier"] = row["fundingDetail/fundingTotal/amount"]

        # ✅ Add department name (with "NOT FOUND" if missing)
        updated_target_df.loc[updated_target_df["JSON Schema 5.0 Data Field"] == "awardeeDetail/departmentName", "Value from the Source or as determined by Supplier"] = row["awardeeDetail/departmentName"]

        # ✅ Add ORCID (with "NOT FOUND" if missing)
        updated_target_df.loc[updated_target_df["JSON Schema 5.0 Data Field"] == "awardeeDetail/affiliationOf/identifier/id/orcid", "Value from the Source or as determined by Supplier"] = row["awardeeDetail/affiliationOf/identifier/id/orcid"]

        # Save output file
        # output_file = os.path.join(output_folder, f"mapped_output_{index + 1}.csv")
        # updated_target_df.to_csv(output_file, encoding="utf-8", index=False)
        # print(f"[SUCCESS] Mapping complete for row {index + 1}. Output saved to: {output_file}")

        # Extract grantAwardId for file naming
        grant_award_id = str(row.get("grantAwardId", f"row_{index + 1}")).strip()

# Ensure the grantAwardId is not empty; use a fallback name if missing
        if not grant_award_id or grant_award_id.lower() == "nan":
            grant_award_id = f"row_{index + 1}"

# Save output file with grantAwardId in the filename
        output_file = os.path.join(output_folder, f"mapped_output_{grant_award_id}.csv")
        updated_target_df.to_csv(output_file, encoding="utf-8", index=False)

        print(f"[SUCCESS] Mapping complete for grantAwardId {grant_award_id}. Output saved to: {output_file}")


# Example usage
map_csv_to_csv("diabeties_uk.csv",
               "./csv_template.csv",
               "./output_files/")