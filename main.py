import os
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
import json

# Load environment variables
load_dotenv()

# Initialize OpenAI client for DeepSeek
client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
)

def read_file_content(file_path):
    """Read content from various file types."""
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == '.csv':
            df = pd.read_csv(file_path)
            return df.to_string()
        elif ext == '.xlsx':
            df = pd.read_excel(file_path)
            return df.to_string()
        elif ext == '.txt':
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        else:
            return f"Unsupported file type: {ext}"
    except Exception as e:
        return f"Error reading {file_path}: {str(e)}"

def read_mysql_data(query):
    """Read data from MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE")
        )
        if connection.is_connected():
            df = pd.read_sql(query, connection)
            return df.to_string()
    except Error as e:
        return f"Error connecting to MySQL: {str(e)}"
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()

def process_with_deepseek(content, template_content, background_info="", context_summary=""):
    """Send content and template to DeepSeek API for processing."""
    prompt = f"""
你是一个专业的数据整理助手。请参照以下提供的模版格式、背景知识和项目进度摘要，对输入的信息进行整理和分析。

【项目背景与术语】:
{background_info}

【进度摘要/上下文】:
{context_summary}

【模版格式】:
{template_content}

【输入信息】:
{content}

请严格按照模版的逻辑和结构输出整理后的内容。如果输入信息中缺少某些字段，请标注为“缺失”或根据上下文合理推断。
"""
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "你是一个专业的数据分析和整理专家,并且精通货物运输。"},
                {"role": "user", "content": prompt}
            ],
            stream=False
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"API Error: {str(e)}"

def update_summary(summary_file, last_action_description):
    """Update context_summary.md with a concise summary of the last action."""
    prompt = f"请为以下操作提供一个极其简短的中文摘要（不超过30字），用于记录项目进度：\n{last_action_description}"
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "你是一个负责项目记录的助手，擅长编写精简的摘要。"},
                {"role": "user", "content": prompt}
            ],
            stream=False
        )
        summary = response.choices[0].message.content.strip()
        with open(summary_file, 'a', encoding='utf-8') as f:
            f.write(f"\n- {summary}")
        print(f"Updated summary in {summary_file}")
    except Exception as e:
        print(f"Failed to update summary: {str(e)}")

def main():
    doc_dir = "doc"
    template_dir = "template"
    result_dir = "result"
    background_file = "background_knowledge.md"
    summary_file = "context_summary.md"

    # Check directories
    for d in [doc_dir, template_dir, result_dir]:
        if not os.path.exists(d):
            os.makedirs(d)

    # Read background and context summary
    background_info = ""
    if os.path.exists(background_file):
        with open(background_file, 'r', encoding='utf-8') as f:
            background_info = f.read()

    context_summary = ""
    if os.path.exists(summary_file):
        with open(summary_file, 'r', encoding='utf-8') as f:
            context_summary = f.read()

    # Get templates
    templates = [f for f in os.listdir(template_dir) if not f.startswith('.')]
    if not templates:
        print("No template found in 'template' folder. Please add a template file.")
        return

    # Use the first template for now
    template_path = os.path.join(template_dir, templates[0])
    template_content = read_file_content(template_path)

    # Process files in doc folder
    files_to_process = [f for f in os.listdir(doc_dir) if not f.startswith('.')]
    if not files_to_process:
        print("No files found in 'doc' folder to process.")
        return

    processed_files = []
    for filename in files_to_process:
        print(f"Processing {filename}...")
        file_path = os.path.join(doc_dir, filename)
        content = read_file_content(file_path)
        
        result = process_with_deepseek(content, template_content, background_info, context_summary)
        
        # Save result
        output_filename = f"result_{filename}.txt"
        output_path = os.path.join(result_dir, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(result)
        print(f"Saved result to {output_path}")
        processed_files.append(filename)

    # Update summary after all files are processed
    if processed_files:
        update_summary(summary_file, f"处理了文件: {', '.join(processed_files)}")

if __name__ == "__main__":
    main()
