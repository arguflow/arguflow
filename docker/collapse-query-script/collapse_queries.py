import clickhouse_connect
import os
import dotenv
import uuid
from typing import Optional
import datetime


def get_search_queries(
    client, dataset_id: uuid.UUID, limit=5000, offset=Optional[datetime.datetime]
):
    query = """
        SELECT id, query, top_score, created_at, search_type, request_params, latency, results, query_vector, is_duplicate, query_rating, dataset_id
        FROM default.search_queries 
        WHERE dataset_id = '{}' AND is_duplicate = 0 AND search_type != 'rag'
        ORDER BY created_at DESC
        LIMIT {}
        """.format(
        str(dataset_id), limit
    )
    if offset is not None:
        query = """
        SELECT id, query, top_score, created_at, search_type, request_params, latency, results, query_vector, is_duplicate, query_rating, dataset_id
        FROM default.search_queries 
        WHERE dataset_id = '{}'
            AND created_at >= '{}' AND search_type != 'rag'
        ORDER BY created_at DESC
        LIMIT {}
        """.format(
            str(dataset_id),
            datetime.datetime.utcfromtimestamp(offset).isoformat(),
            limit,
        )
    vector_result = client.query(query, query_formats={"Date*": "int"})
    return vector_result.result_rows


def get_datasets(client):
    query = "SELECT DISTINCT dataset_id FROM default.search_queries"
    dataset_result = client.query(query)
    return dataset_result.result_rows


def get_dataset_last_collapsed(client, dataset_id: uuid.UUID):
    query = """
        SELECT last_collapsed
        FROM default.last_collapsed_dataset
        WHERE dataset_id = '{}'
    """.format(
        str(dataset_id)
    )
    dataset_result = client.query(query, query_formats={"Date*": "int"})
    row = dataset_result.result_rows
    return row[0][0] if row else None


def set_dataset_last_collapsed(
    client, dataset_id: uuid.UUID, last_collapsed: datetime.datetime
):
    client.insert(
        "last_collapsed_dataset",
        [
            [
                uuid.uuid4(),
                last_collapsed,
                dataset_id,
                datetime.datetime.now(),
            ]
        ],
        column_names=[
            "id",
            "last_collapsed",
            "dataset_id",
            "created_at",
        ],
    )


def collapse_queries(rows, time_window=5):
    if not rows:
        return []

    # Sort rows by timestamp
    sorted_rows = sorted(rows, key=lambda x: x[3])

    # Group queries that might be part of the same typing sequence
    typing_sequences = []
    current_sequence = [sorted_rows[0]]

    for i in range(1, len(sorted_rows)):
        current_row = sorted_rows[i]
        prev_row = current_sequence[-1]

        time_diff = current_row[3] - prev_row[3]
        current_query = current_row[1].strip().lower()
        prev_query = prev_row[1].strip().lower()

        # Check if queries are related by either:
        # 1. One being a prefix of the other
        # 2. Having a significant overlap (for handling backspace/corrections)
        is_related = (
            current_query.startswith(prev_query)
            or prev_query.startswith(current_query)
            or (
                len(set(current_query) & set(prev_query))
                / max(len(current_query), len(prev_query))
                > 0.8
            )
        )

        if time_diff <= time_window and is_related:
            current_sequence.append(current_row)
        else:
            typing_sequences.append(current_sequence)
            current_sequence = [current_row]

    typing_sequences.append(current_sequence)

    # For each sequence, keep only the longest query and mark others as duplicates
    rows_to_be_deleted = []
    for sequence in typing_sequences:
        if len(sequence) > 1:
            # Find the longest query in the sequence
            longest_row = max(sequence, key=lambda x: len(x[1].strip()))

            # Mark all other queries in the sequence as duplicates
            for row in sequence:
                if row != longest_row:
                    rows_to_be_deleted.append(row)

    return rows_to_be_deleted


def insert_duplicate_rows(client, rows):
    if rows:
        duplicate_rows = [list(row) for row in rows]
        for row in duplicate_rows:
            row[9] = 1  # Set is_duplicate to 1
        client.insert(
            "search_queries",
            duplicate_rows,
            column_names=[
                "id",
                "query",
                "top_score",
                "created_at",
                "search_type",
                "request_params",
                "latency",
                "results",
                "query_vector",
                "is_duplicate",
                "query_rating",
                "dataset_id",
            ],
        )


def main():
    dotenv.load_dotenv()
    client = clickhouse_connect.get_client(dsn=os.getenv("CLICKHOUSE_DSN"))

    try:
        datasets = get_datasets(client)

        for dataset in datasets:
            dataset_id = dataset[0]
            last_collapsed = get_dataset_last_collapsed(client, dataset_id)

            print("Collapsing dataset", dataset_id, "from", last_collapsed)

            num_duplicates = 0
            rows = get_search_queries(client, dataset_id, 5000, last_collapsed)

            while rows:
                last_collapsed = rows[0][3]

                duplicates = collapse_queries(rows)
                num_duplicates += len(duplicates)

                insert_duplicate_rows(client, duplicates)

                new_rows = get_search_queries(client, dataset_id, 5000, last_collapsed)
                if new_rows and new_rows[-1][0] == rows[-1][0]:
                    break
                rows = new_rows

            if last_collapsed:
                set_dataset_last_collapsed(client, dataset_id, last_collapsed)

            print(f"Processed dataset {dataset_id}, marked {num_duplicates} duplicates")
            print()

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
