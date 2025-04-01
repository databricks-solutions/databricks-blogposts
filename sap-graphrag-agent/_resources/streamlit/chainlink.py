import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
import os


# INTITIALIZE CONNECTIONS AND FUNCTIONS

# Parse agent output for Streamlit message box
import re

def parse_output(output):
    # Parse the output string into components
    query = output.split('\n')[0]
    tool_calls = re.findall(r'<tool_call>(.*?)</tool_call>', output, re.DOTALL)
    tool_results = re.findall(r'<tool_call_result>(.*?)</tool_call_result>', output, re.DOTALL)
    final_answer = output.split('\n')[-3]
    
    return query, tool_calls, tool_results, final_answer

def extract_final_answer(output_text: str) -> str:
    """Extracts text after last XML-like tag closure."""
    # Split by closing tags and take last segment
    parts = output_text.rsplit('</tool_call_result>', 1)
    # Clean up remaining whitespace and empty lines
    return parts[-1].strip() if len(parts) > 1 else output_text.strip()


# CREATE STREAMLIT APP LAYOUT
# QUERY AND DRAW GRAPH CAPABILITY

st.title("ChainLink :bike: Bike sales assistant")

# CHAT CAPABILITY

# Initialize the Databricks Workspace Client (local deployment)
#w = WorkspaceClient(host=os.getenv('DATABRICKS_HOST'), token='<ENTER PERSONAL ACCESS TOKEN>')
# For Databricks Apps deployment
w = WorkspaceClient()

if "visibility" not in st.session_state:
    st.session_state.visibility = "visible"
    st.session_state.disabled = False

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

show_tool_hist = st.toggle('Show Tool Output')

# Accept user input
if prompt := st.chat_input("Ask a question about SAP bike sales data..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    messages = [ChatMessage(role=ChatMessageRole.USER, content=prompt)]

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        # Query the Databricks serving endpoint
        try:
            response = w.serving_endpoints.query(
                name=os.getenv("DATABRICKS_SERVING_ENDPOINT"),
                messages=messages,
                max_tokens=400,
            )
            assistant_response = response.choices[0].message.content
            if show_tool_hist:
                final_answer = assistant_response
            else:
                final_answer = extract_final_answer(assistant_response)
            st.markdown(final_answer)
        except Exception as e:
            st.error(f"Error querying model: {e}")

    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": final_answer})