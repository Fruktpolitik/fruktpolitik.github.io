from pathlib import Path
import os
from openai import OpenAI

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

prompt = Path("rss_prompt.txt").read_text(encoding="utf-8")

response = client.responses.create(
    model=os.getenv("OPENAI_MODEL", "gpt-5.5"),
    tools=[
        {"type": "web_search"}
    ],
    input=prompt,
)

output_file = Path(os.getenv("OUTPUT_FILE", "public/newsfeed.xml"))
output_file.parent.mkdir(parents=True, exist_ok=True)
output_file.write_text(response.output_text.strip() + "\n", encoding="utf-8")
