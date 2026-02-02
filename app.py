import streamlit as st
import pandas as pd
import asyncio
from crawl4ai import AsyncWebCrawler
import re
from huggingface_hub import InferenceClient
import io
import time
from urllib.parse import urlparse
import traceback

# Windows asyncio fix
import sys
# ─── Install Playwright browsers if running in cloud ───
import os

# Very simple cloud detection (works most of the time)
import os
import sys
import subprocess

# Windows fix (keep if needed)
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# ─── Install Playwright browsers on Streamlit Cloud ───
if "STREAMLIT" in os.environ or os.getenv("IS_STREAMLIT_CLOUD"):
    try:
        # First try with deps (sometimes more reliable)
        subprocess.run(["playwright", "install-deps"], check=True, capture_output=True)
        subprocess.run(["playwright", "install", "chromium"], check=True, capture_output=True)
        print("Playwright chromium installed successfully (cloud).")
    except subprocess.CalledProcessError as e:
        print(f"Playwright install failed: {e.stderr.decode()}")
        # Fallback – minimal install
        os.system("playwright install chromium --with-deps")
    except Exception as e:
        print(f"Playwright setup error: {e}")
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
os.environ["PYTHONIOENCODING"] = "utf-8"

st.set_page_config(page_title="Website → Industry & About Extractor", layout="wide")

st.title("Website Info Extractor (Qwen2.5-7B-Instruct + Crawl4AI)")
st.caption("Extracts industry, short description & about us using real browser rendering")

# ────────────────────────────────────────────────
# Session state keys
# ────────────────────────────────────────────────
if 'df' not in st.session_state:
    st.session_state.df = None
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'stop_requested' not in st.session_state:
    st.session_state.stop_requested = False

# ────────────────────────────────────────────────
# Sidebar
# ────────────────────────────────────────────────
with st.sidebar:
    st.subheader("Settings")
    hf_token = st.text_input("Hugging Face Token", type="password",
                             help="Required – get it from https://huggingface.co/settings/tokens")
    
    max_chars = st.slider("Max characters sent to LLM", 4000, 16000, 10000, step=1000)
    
    st.markdown("---")
    st.info("Crawl4AI uses real browser → good for JS-heavy sites\nFirst run may be slow (browser download)")

# ────────────────────────────────────────────────
# File upload
# ────────────────────────────────────────────────
uploaded_file = st.file_uploader("Upload Excel / CSV (needs 'website' column)",
                                 type=["xlsx", "xls", "csv"])

if uploaded_file is not None and st.session_state.df is None:
    try:
        if uploaded_file.name.lower().endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        # Find website column
        col_map = {str(c).lower().strip(): c for c in df.columns}
        website_col = next((v for k, v in col_map.items() if k in ['website', 'url', 'web', 'link', 'site']), None)
        
        if website_col is None:
            st.error("No column like 'website', 'url', 'link', etc. found.")
            st.stop()

        df = df.rename(columns={website_col: 'website'})

        # Prepare output columns
        target_cols = ['industry', 'small description', 'about us', 'status']
        for col in target_cols:
            if col not in df.columns:
                df[col] = pd.NA
            df[col] = df[col].astype("string").fillna("")

        st.session_state.df = df
        st.success("File loaded. You can now start processing.")
    except Exception as e:
        st.error(f"File reading failed: {e}")
        st.stop()

# ────────────────────────────────────────────────
# Crawler wrapper
# ────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl="15m")
def crawl_website_sync(url: str, max_chars: int = 10000) -> str:
    async def _crawl():
        async with AsyncWebCrawler(verbose=False) as crawler:
            result = await crawler.arun(
                url=url,
                word_count_threshold=180,
                bypass_cache=True,
                remove_navigation=True,
                remove_footer=True,
                exclude_external_links=True,
            )
            if not result.success or not result.markdown:
                return ""
            return result.markdown.strip()[:max_chars]

    try:
        return asyncio.run(_crawl())
    except Exception as e:
        st.warning(f"Crawl4AI error: {e}")
        return ""

# ────────────────────────────────────────────────
# Download helper
# ────────────────────────────────────────────────
def get_download_button(df: pd.DataFrame, filename="websites_enriched.xlsx"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name="Extracted")
    output.seek(0)
    return st.download_button(
        label="📥 Download Current Results",
        data=output,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

# ────────────────────────────────────────────────
# Main processing logic
# ────────────────────────────────────────────────
if st.session_state.df is not None:
    df = st.session_state.df

    col1, col2 = st.columns([3,1])
    with col1:
        start_btn = st.button("🚀 Start / Resume Processing", type="primary", disabled=st.session_state.processing)
    with col2:
        stop_btn = st.button("⏹️ Stop Processing", type="secondary", disabled=not st.session_state.processing)

    if stop_btn:
        st.session_state.stop_requested = True
        st.warning("Stop requested — finishing current row...")

    if start_btn and hf_token.strip():
        st.session_state.processing = True
        st.session_state.stop_requested = False

        try:
            client = InferenceClient(
                model="Qwen/Qwen2.5-7B-Instruct",
                token=hf_token,
                provider="auto",
            )
            st.info("LLM client initialized.")
        except Exception as e:
            st.error(f"Cannot initialize Hugging Face client: {e}")
            st.session_state.processing = False
            st.stop()

        progress_bar = st.progress(0)
        status_text = st.empty()

        total = len(df)
        processed_count = 0

        # Count already finished rows
        finished = df['status'].isin(["OK", "CRAWL_TOO_SHORT", "SKIPPED (invalid/empty URL)", "ERROR", "API_LIMIT_REACHED"]).sum()
        processed_count = finished

        def normalize_url(raw):
            if pd.isna(raw) or not str(raw).strip():
                return ""
            raw = str(raw).strip().lower()
            raw = re.sub(r'^https?:/{2,}', 'https://', raw)
            raw = re.sub(r'^wwww?\.', 'www.', raw)
            raw = re.sub(r'^www{2,}\.', 'www.', raw)
            if not re.match(r'^https?://', raw):
                raw = 'https://' + raw.lstrip(':/')
            return raw.rstrip('/')

        for idx, row in df.iterrows():
            if st.session_state.stop_requested:
                status_text.warning("Processing stopped by user.")
                break

            if row['status'] in ["OK", "CRAWL_TOO_SHORT", "SKIPPED (invalid/empty URL)", "ERROR", "API_LIMIT_REACHED"]:
                continue  # already processed

            raw_url = row.get('website', '')
            url = normalize_url(raw_url)

            if not url:
                df.at[idx, 'status'] = "SKIPPED (invalid/empty URL)"
                processed_count += 1
                continue

            status_text.text(f"Processing {processed_count+1}/{total}: {url}")

            try:
                crawled_text = crawl_website_sync(url, max_chars=max_chars)
                st.write(f"Debug → {url} | crawled chars: {len(crawled_text)}")

                if len(crawled_text) < 400:
                    df.at[idx, 'status'] = "CRAWL_TOO_SHORT"
                    processed_count += 1
                    continue

                prompt = f"""You are a precise business intelligence analyst.

Using ONLY the website content below, extract:

1) Industry  
   - One clear, specific industry or sector
   - Avoid generic terms like "Manufacturing" alone

2) Small description  
   - 1–2 concise sentences
   - What the company actually does or sells

3) About us  
   - 4–8 professional sentences
   - Prefer factual company information
   - Do NOT invent history, years, or claims

Rules:
- If information is missing, infer conservatively
- Do not hallucinate
- No markdown, no bullet points

Output EXACTLY in this format:

Industry: <text>
Small description: <text>
About us: <text>

Website content:
{crawled_text}""".strip()

                messages = [{"role": "user", "content": prompt}]

                with st.spinner(f"LLM → {urlparse(url).netloc}"):
                    completion = client.chat.completions.create(   # updated method name
                        messages=messages,
                        max_tokens=650,
                        temperature=0.2
                    )
                    answer = completion.choices[0].message.content.strip()

                # Parse answer
                industry = small = about = ""
                capturing_about = False

                for line in answer.splitlines():
                    line = line.strip()
                    if line.startswith("Industry:"):
                        industry = line[9:].strip()
                    elif line.startswith("Small description:"):
                        small = line[18:].strip()
                    elif line.startswith("About us:"):
                        about = line[9:].strip()
                        capturing_about = True
                    elif capturing_about and line:
                        about += " " + line

                df.at[idx, 'industry'] = industry
                df.at[idx, 'small description'] = small
                df.at[idx, 'about us'] = about
                df.at[idx, 'status'] = "OK"

                processed_count += 1

            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in ["402", "payment required", "insufficient credit", "quota", "rate limit"]):
                    df.at[idx, 'status'] = "API_LIMIT_REACHED"
                    status_text.error(
                        "Hugging Face returned 402 / quota error → probably out of free credits.\n"
                        "→ Try a different HF token or wait until quota resets."
                    )
                    st.session_state.stop_requested = True
                else:
                    df.at[idx, 'status'] = "ERROR"
                    st.error(f"Row {idx+1} failed: {type(e).__name__} – {str(e)[:180]}")

                processed_count += 1

            # Live progress
            progress_bar.progress(min(processed_count / total, 1.0))

            # Save partial result every row (important!)
            st.session_state.df = df.copy()

            time.sleep(0.5)  # polite + gives stop button chance to register

            if st.session_state.stop_requested:
                break

        st.session_state.processing = False
        st.session_state.stop_requested = False

        status_text.success(f"Finished / Stopped – processed {processed_count} rows.")

    # Always show current download button when we have data
    if st.session_state.df is not None:
        get_download_button(st.session_state.df)

    if st.session_state.df is not None:
        with st.expander("Preview current table"):
            st.dataframe(st.session_state.df)

elif not hf_token.strip() and uploaded_file is not None:
    st.warning("Please enter your Hugging Face token to start processing.")
else:

    st.info("Upload your file containing websites to begin.")
