import json
import logging
import os
import random
import requests
import streamlit as st
import time

from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from mlflow.deployments import get_deploy_client

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the Databricks Client
client = get_deploy_client("databricks")

# Ensure environment variable is set correctly
assert os.getenv('SERVING_ENDPOINT'), "SERVING_ENDPOINT must be set in app.yaml."

def get_user_info():
    headers = st.context.headers
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
    )

user_info = get_user_info()


# Streamlit app
if "visibility" not in st.session_state:
    st.session_state.visibility = "visible"
    st.session_state.disabled = False

st.title("✨ Ad Profile Agent ✨")
st.write(f"Speak to an ad profile assistant based on your audience personas.")

# Avatars
human_avatar = "💬"
tribe_avatars = {
    "The Homebodies (Suburban Family-Oriented)": "🧑🏼‍🍼",
    "The Luxe Lifers (High-Income Empty Nester)": "🏌🏻‍♀️",
    "The Campus Creatives (College Student)": "👨🏽‍🎓",
    "The Quiet Seekers (Retired Rural Dweller)": "👨🏼‍🌾",
    "The Innovators (Tech-Savvy Professional)": "👩🏼‍💻"
}
# Dropdown for selecting tribe
tribe_options = list(tribe_avatars.keys())

# Initialise session state for tribe selection
if "selected_tribe" not in st.session_state:
    st.session_state.selected_tribe = tribe_options[0] # Default value

# Directly link selectbox to session state
st.selectbox(
    "Select a customer tribe:", tribe_options, 
    index=tribe_options.index(st.session_state.selected_tribe), 
    key="selected_tribe"
)
st.divider()

# Only reset chat when the selection actually changes
if st.session_state.selected_tribe != st.session_state.get("prev_selected_tribe", None):
    st.session_state.messages = []  # Clear chat history
    st.session_state.prev_selected_tribe = st.session_state.selected_tribe  # Track previous selection

# Initialise chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"], avatar=message.get("avatar", "")):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("How can I help with your ad campaign?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt, "avatar": human_avatar})
    # Display user message in chat message container
    with st.chat_message("user", avatar=human_avatar):
        st.markdown(prompt)

    # messages = [{'content': prompt, 'role': 'user'}]

    # Display assistant response in chat message container
    with st.chat_message("assistant", avatar=tribe_avatars[st.session_state.selected_tribe]):
        # Query the Databricks serving endpoint
        try:
            response = client.predict(
                endpoint=os.getenv("SERVING_ENDPOINT"),
                inputs={
                    "messages": [{"role": msg["role"], "content": msg["content"]} for msg in st.session_state.messages], 
                    "custom_inputs": {"tribe": st.session_state.selected_tribe},
                },
            )
            assistant_response = response['messages'][0]['content']

            # Display one word at a time
            message_placeholder = st.empty()  # Creates a placeholder for typing effect
            full_response = ""

            words = assistant_response.split(" ")  # Split into words
            for word in words:
                full_response += word + " "  # Append word with space

                # Preserve line breaks properly
                formatted_response = full_response.replace("\n", "<br>")

                # Update message dynamically
                message_placeholder.markdown(
                    f"<p style='white-space: pre-wrap;'>{formatted_response}</p>", 
                    unsafe_allow_html=True
                )

                # Simulate natural typing speed variation
                time.sleep(random.uniform(0.02, 0.10))
        except Exception as e:
            st.error(f"Error querying model: {e}")

    # Add assistant response to chat history
    st.session_state.messages.append({
        "role": "assistant",
        "content": assistant_response,
        "avatar": tribe_avatars[st.session_state.selected_tribe]
    })
