from flask import Flask, render_template, request, redirect, url_for, flash
import os
import pandas as pd
from dotenv import load_dotenv
from analyzer import api, preprocessing, storage
from urllib.parse import urlparse

load_dotenv()

app = Flask(__name__)
app.secret_key = 'protenderizer-secret-key'


def parse_db_url(url):
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port,
        'database': parsed.path[1:],
        'user': parsed.username,
        'password': parsed.password
    }


@app.route("/", methods=["GET", "POST"])
def fetch():
    message = ""
    if request.method == "POST":
        start_date = request.form.get("start_date")
        end_date = request.form.get("end_date")
        output = request.form.get("output", "db")
        db_url = os.getenv("DB_URL")
        db_table = request.form.get("db_table", "notices")

        if not start_date or not end_date:
            flash("Start date and end date are required.", "error")
            return redirect(url_for("fetch"))

        client = api.TEDAPIClient()
        query = client.build_query(start_date, end_date)

        output_file = None
        if output in ["csv", "json"]:
            output_file = f"notices.{output}"

        store_to_db = output == "db"
        db_options = {
            "config": parse_db_url(db_url),
            "table": db_table,
            "preprocess": True
        } if store_to_db else None

        try:
            notices = client.fetch_all_scroll(
                query=query,
                output_file=output_file,
                output_format=output,
                store_db=store_to_db,
                db_options=db_options
            )
            if store_to_db:
                message = f"Stored {len(notices)} records to database table '{db_table}'."
            else:
                message = f"Saved {len(notices)} records to file '{output_file}'."
        except Exception as e:
            flash(f"Fetch failed: {str(e)}", "error")
            return redirect(url_for("fetch"))

    return render_template("fetch.html", message=message)


if __name__ == "__main__":
    app.run(debug=True)
