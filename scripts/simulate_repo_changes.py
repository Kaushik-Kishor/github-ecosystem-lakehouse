
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def simulate_changes():
    load_dotenv(dotenv_path=Path(".env"))

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="app",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    try:
        with conn:
            with conn.cursor() as cur:
                # 1. UPDATE an existing repository
                existing_repo_id = 140

                cur.execute(
                    """
                    UPDATE public.repos
                    SET stars = stars + 100,
                        updated_at = NOW()
                    WHERE repo_id = %s
                    RETURNING repo_id, name, stars;
                    """,
                    (existing_repo_id,),
                )

                updated_repo = cur.fetchone()

                if updated_repo is None:
                    raise RuntimeError(
                        f"Repository {existing_repo_id} was not found."
                    )

                print(
                    f"UPDATE: repo_id={updated_repo[0]}, "
                    f"name={updated_repo[1]}, stars={updated_repo[2]}"
                )

                # 2. INSERT a temporary repository
                cur.execute(
                    "SELECT COALESCE(MAX(repo_id), 0) + 1 FROM public.repos;"
                )
                temp_repo_id = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO public.repos
                        (repo_id, name, owner, stars, is_archived, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    RETURNING repo_id, name, owner;
                    """,
                    (
                        temp_repo_id,
                        "nexus-cdc-test",
                        "nexus-test",
                        42,
                        False,
                    ),
                )

                inserted_repo = cur.fetchone()

                print(
                    f"INSERT: repo_id={inserted_repo[0]}, "
                    f"name={inserted_repo[1]}, owner={inserted_repo[2]}"
                )

                # 3. DELETE only the temporary repository
                cur.execute(
                    """
                    DELETE FROM public.repos
                    WHERE repo_id = %s
                    RETURNING repo_id, name;
                    """,
                    (temp_repo_id,),
                )

                deleted_repo = cur.fetchone()

                if deleted_repo is None:
                    raise RuntimeError("Temporary repository deletion failed.")

                print(
                    f"DELETE: repo_id={deleted_repo[0]}, "
                    f"name={deleted_repo[1]}"
                )

        print("\nTransaction committed successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    simulate_changes()
