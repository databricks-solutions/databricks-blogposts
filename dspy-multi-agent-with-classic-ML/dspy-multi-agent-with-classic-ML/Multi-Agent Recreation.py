# Databricks notebook source
# MAGIC %md
# MAGIC #Step 0: Step up
# MAGIC
# MAGIC Ensure you are using at least Databricks Runtime 14.3 LTS ML
# MAGIC
# MAGIC The cell below will do the following: 
# MAGIC 1. Set up a cpu model serving endpoint for the Microsoft BeIT model 
# MAGIC 2. Set up a cpu model serving endpoint for the Tesseract OCR model 
# MAGIC
# MAGIC Please adjust the config file in the folder config to make sure your models are saved in the right location. 

# COMMAND ----------

# DBTITLE 1,Installs Packages and Serves ML Models
# MAGIC %run ./config/init

# COMMAND ----------

# MAGIC %md
# MAGIC #RAG Set up (optional)
# MAGIC
# MAGIC If you don't have an existing VectorSearchEndpoint, use the line below. **Note, this will clear your schema so make sure to use a test_schema in the config file**
# MAGIC
# MAGIC Otherwise, set the Index name and Endpoint name of the existing RAG endpoint you have  

# COMMAND ----------

# %run ./config/rag_setup/setup

# COMMAND ----------

# DBTITLE 1,Your RAG details
# VECTOR_SEARCH_ENDPOINT_NAME = "your endpoint name goes here"
# vectorSearchIndexName = "your index name goes here"

# COMMAND ----------

# DBTITLE 1,Set your API keys
from openai import OpenAI
import json
import os

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
OPENAI_TOKEN = dbutils.secrets.get(scope="groq_key", key="openai") #optional
ANTHROPIC_API_KEY = dbutils.secrets.get(scope="groq_key", key="anthropic")
base_url = f'https://{spark.conf.get("spark.databricks.workspaceUrl")}/serving-endpoints'

# COMMAND ----------

# MAGIC %md
# MAGIC #Overview
# MAGIC This demo will walk you through creating Agents on DSPy. You will need an understanding of how DSPy Signature and DSPy Modules work. Please review the associated Medium article for links to an introduction to DSPy or go to dspy.ai for tutorials! 
# MAGIC
# MAGIC We will break down the notebook into the following pieces: 
# MAGIC 1. Designing a DSPy Signature
# MAGIC 2. Designing a DSPy Module 
# MAGIC 3. The logic behind some of the modules
# MAGIC 4. Putting is altogether in one big DSPy module. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set the LLM 
# MAGIC
# MAGIC Before we get started, DSPy has a way to set a "global" LLM that all its modules, including your custom ones, can use. You can also set different LLMs so that you can choose when to use each LLM. This is entirely optional but a few are shown below as an example. 
# MAGIC
# MAGIC You can change this to any LLM supported by LiteLLM. Note that this is a global configuration and there are ways to set LLMs for specific modules

# COMMAND ----------

import dspy
llama = dspy.LM('databricks/databricks-meta-llama-3-3-70b-instruct', cache=False)
mixtral = dspy.LM('databricks/databricks-mixtral-8x7b-instruct', cache=False)
dbrx = dspy.LM('databricks/databricks-dbrx-instruct', cache=False)
llama8b = dspy.LM('databricks/llama8b', cache=False)
claude = dspy.LM('anthropic/claude-3-5-sonnet-20241022', api_key=ANTHROPIC_API_KEY, cache=False)
gpt4o = dspy.LM('openai/gpt-4o', api_key=OPENAI_TOKEN)
dspy.configure(lm=llama) #this is to set a default global model

# COMMAND ----------

# MAGIC %md
# MAGIC The Medium article uses a Claude endpoint that is using AI Gateway. You can define the same endpoint by serving it on Model serving and using the name of the model serving endpoint instead. See the example below

# COMMAND ----------

import dspy
databricks_claude = dspy.LM('databricks/austin-ai-gateway', cache=False)
dspy.configure(lm=databricks_claude)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set the Specific Model Each Agent uses 
# MAGIC
# MAGIC To make LLM selection a little easier, this demo is set up so that you can assign the model and model_name below.
# MAGIC
# MAGIC This may not be clear now (as you need to see how the modules are defined) but this is how you could switch out which Agent uses what LLM depending on the task. 

# COMMAND ----------

#Agent Model Configuration

router_model = llama
router_model_name = llama.model 
sales_model = llama
sales_model_name = llama.model
pokemon_model = llama
pokemon_model_name = llama.model 
vision_model = llama
vision_model_name = llama.model 
databricks_model = llama
databricks_model_name = llama.model 

# COMMAND ----------

#Agent Model Configuration

router_model = claude 
router_model_name = claude.model 
sales_model = claude
sales_model_name = claude.model 
pokemon_model = claude 
pokemon_model_name = claude.model 
vision_model = claude
vision_model_name = claude.model  
databricks_model = claude
databricks_model_name = claude.model 

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 1: Define DSPy Signatures
# MAGIC
# MAGIC We need to define DSPy Signatures. They're like cooking recipes; the have a list of inputs (ingredients) that eventually end in an output (dish). These signatures are fed into a DSPy Module which we will get into a little later.
# MAGIC
# MAGIC Everything in the Signature influences the prompt DSPy generates. As an overview, this includes: 
# MAGIC 1. The Signature name
# MAGIC 2. The class docstring 
# MAGIC 3. The variable names themselves 
# MAGIC 4. Typing (uses Pydantic)
# MAGIC 5. Hints or descriptions you can put to help clear up expectations on the input or output
# MAGIC
# MAGIC This will enforce typing and create prompts to enforce said typing based on your task. We can take advantage of this later in an optimizer workload (but an optimization tutorial is out of scope for this demo). 
# MAGIC
# MAGIC At a minimum, we need to clear define what the Signature is about, which we can do in the docstring of the Signature. Then, we need at least one InputField and one OutputField. This will enforce the typing and always give the Outputs you're looking for 
# MAGIC
# MAGIC So, if. you had one InputField that expects a string and 3 OutputFields that expect a string, dict, int, then the DSPy Module will enforce the initial input as a string and return a string, dict and int that you can programatically access. 
# MAGIC
# MAGIC Let's start by defining our first signature for our first agent: router_agent 

# COMMAND ----------

import dspy
from typing import Literal, Optional
class router_agent(dspy.Signature):
  """Given the input from the user, review the conversation_history if available and determine the next agent to send the user to"""
  question: str = dspy.InputField()
  conversation_history: Optional[list] = dspy.InputField()
  next_agent: Literal['pokemon_expert','databricks_expert','image_expert', 'sales_expert'] = dspy.OutputField(desc="the next agent")

# COMMAND ----------

# MAGIC %md
# MAGIC You can see the router_agent signature is a class. It expects a string question and the conversation_history as a list but can be optional. It will always output the next_agent as pokemon_expert, databricks_expert, image_expert and sales_expert. There's an added hint to the next_agent as a desc keyword clearing up that this will decide on what the next agent is. 
# MAGIC
# MAGIC The docstring clearly states that this is to determine what the next agent is. 
# MAGIC
# MAGIC You can get more creative with this and add more inputs and outputs. However, the general guidance is to keep it simple and focused on a task. The more you ask it to do in one Signature, the longer and harder it is to manage 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Make the rest of the Signatures
# MAGIC
# MAGIC We will make the rest of the Signatures to complete this demo. This Signatures are: 
# MAGIC 1. Pokemon_agent: a signature designed to identify the pokemon in the question, hit an external Pokemon database for more accurate information and then select which agent to go to next. It will stay on the Pokemon Agent if the conversation is still about the Pokemon 
# MAGIC 2. Databricks_agent: a signature designed to answer questions around Databricks using RAG. This is where the VectorSearchEndpoint and Index will be used to answer questions about Databricks documentation. **However** if you put your own RAG bot here, it would be puling information from that RAG bot 
# MAGIC 3. Sales_agent: a signature designed to simulate a sales conversation. It exists to demonstrate how we can break mid conversation with an agent and go to a new agent 
# MAGIC 4. Vision_agent: a signature designed to understand the user's query, find the image url and determine which vision model to use to complete the user's task. For this demo, we had you serve a Tesseract and BeIT Vision Transformer model to do OCR and Image Classification repsectively. So, the vision agent will determine which of these tasks will best complete the user's request. 
# MAGIC
# MAGIC All the signatures determine if another agent needs to be called. Additionally, they also save the input of the user and response they generated into history to save as "memory". This could likely be improved. 

# COMMAND ----------

import dspy
from typing import Literal, Optional

class pokemon_agent(dspy.Signature):
  """
  Review the conversation_history before starting the routine. If it's a transfer to you, greet the user and resume the conversation. 
  If at any point the question is not about pokemon, do not answer the question and say you are transferring the user to the right agent and transfer them to the next_agent
  The routine: 
  1. Identify the pokemon in the pokemon_question and only use the information from the pokemon_lookup to answer the question about the pokemon.
  2. Add your reponse to the history in this format {'role': 'assistant', 'content': your answer here}. 
  3. If the user is satisfied or the user asks a question not related to pokemon, select the proper next_agent. Use the router_agent if you're not sure. Use pokemon_agent if not required. """ 
  pokemon_question: str = dspy.InputField(desc='this will always have a pokemon in the sentence') 
  conversation_history: list = dspy.InputField()
  response: str = dspy.OutputField() 
  history: dict = dspy.OutputField() 
  next_agent: Literal['pokemon_expert','databricks_expert','image_expert', 'sales_expert', 'router_agent'] = dspy.OutputField()

class databricks_agent(dspy.Signature):
  """
  Review the conversaiton_history for any relevant context. If it's a transfer to you, greet the user and resume the conversation.
  Review the question and follow this routine: 
  1. Determine what the question is
  2. Optionally, Create additional distinct and different questions, up to 3, to help pull more relatable information
  3. If there are subquestions, call the databricks documentation tool multiple times to create the answer 
  4. Use the retrieved information from the databricks documentation tool to create the answer
  5. Add your reponse to the history in this format {'role': 'assistant', 'content': your answer here}
  6. If the user is satisfied or the user asks a question not related to Databricks, select the proper next_agent. Use the router_agent if you're not sure. Use databricks_agent if not required
  If at any point the question is not about Databricks, do not answer the question and say you are transferring the user to the right agent and transfer them to the next_agent 
  """ 
  databricks_question: str = dspy.InputField() 
  conversation_history: list = dspy.InputField()
  response: str = dspy.OutputField() 
  history: dict = dspy.OutputField() 
  next_agent: Literal['pokemon_expert','databricks_expert','image_expert', 'sales_expert', 'router_agent'] = dspy.OutputField()

class sales_agent(dspy.Signature):
  """
  "You are a sales agent for ACME Inc."
  "Always answer in a sentence or less."
  "If at any point the question is not about sales, do not answer the question and say you are transferring the user to the right agent and transfer them to the next_agent"
  "Review the conversation_history before starting the routine. If it's a transfer to you, greet the user and resume the conversation."
  "Follow the following routine with the user:"
  "1. Ask them about any problems in their life related to catching roadrunners.\n"
  "2. Casually mention one of ACME's crazy made-up products can help. Use look_up_item to see if it exists\n"
  " - Don't mention price.\n"
  "3. Confirm if the user wants to buy the item\n"
  "4. Only after everything, and if the user says yes, execute_order and a crazy caveat\n"
  "5. Add your reponse to the history in this format {'role': 'assistant', 'content': your answer here}"
  "6. If the user is satisfied or the user asks a question not related to sales, select the proper next_agent. Use the router_agent if you're not sure. Use sales_agent if not required"
  """ 
  user_question: str = dspy.InputField() 
  conversation_history: list = dspy.InputField()
  response: str = dspy.OutputField() 
  history: dict = dspy.OutputField()
  next_agent: Literal['pokemon_expert','databricks_expert','image_expert', 'sales_expert', 'router_agent'] = dspy.OutputField()

class vision_agent(dspy.Signature):
  """
  "You are an expert vision model selection agent
  "Review the conversation_history for relevant context. If it's a transfer to you, greet the user and resume the conversation."
  "If at any point the question is not about vision models, do not answer the question and say you are transferring the user to the right agent and transfer them to the next_agent"
  "Follow the following routine with the user:"
  "1. Identify the task based on the user's question. It must be ocr or classification."
  "2. Identify the url in the user's question" 
  "3. Send the url and task to the vision_transformer tool as url and task" 
  "4. Respond the the tool output"
  "5. Add your response to the history in this format {'role': 'assistant', 'content': your answer here}"
  "6. Continue assisting the user until otherwise." 
  "7. If the user is satisfied or the user asks a question not related to vision models, select the proper next_agent. Use the router_agent if you're not sure. Use image_expert if not required"
  """ 
  user_question: str = dspy.InputField() 
  conversation_history: Optional[list] = dspy.InputField()
  response: str = dspy.OutputField() 
  history: dict = dspy.OutputField() 
  next_agent: Literal['pokemon_expert','databricks_expert','image_expert', 'sales_expert', 'router_agent'] = dspy.OutputField()



# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: DSPy module
# MAGIC
# MAGIC We now have to feed these signatures into a DSPy module. DSPy has the following modules that you can use: 
# MAGIC 1. DSPy.Predict: The most simple and straightforward one 
# MAGIC 2. DSPy.ChainOfThought: This adds additional LLM calls to imitate Chain of Through reasoning on any LLM 
# MAGIC 3. DSPy.ReAct: ReAct is an acronym for Reasoning and Action. This is the basis for any tool calling and is what the paper that named this is called. We must use this for any signatures that require a tool call, which is almost all of them 
# MAGIC
# MAGIC Note, DSPy is still maturing and some of the more advance modules are still maturing. 
# MAGIC
# MAGIC Let's walk through router_agent for a simple example, and then the pokemon signature as a tool calling example
# MAGIC

# COMMAND ----------

# DBTITLE 1,Using DSPy Predict
router_decision = dspy.Predict(router_agent) #This just needs the DSPy Signature to know what to do
result = router_decision(question="I need help with Databricks", conversation_history=None)
result

# COMMAND ----------

# DBTITLE 1,Using DSPy ReAct
# For the pokemon agent, we need to define a python function for the tool so we define it below 
import requests
def pokemon_lookup(pokemon):
    """use to find more information about specific pokemon"""
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon.lower()}"
    response = requests.get(url)
    if response.status_code == 200:
        pokemon_data = response.json()
        pokemon_info = {
            "name": pokemon_data["name"],
            "height": pokemon_data["height"],
            "weight": pokemon_data["weight"],
            "abilities": [ability["ability"]["name"] for ability in pokemon_data["abilities"]],
            "types": [type_data["type"]["name"] for type_data in pokemon_data["types"]],
            "stats_name": [stat['stat']['name'] for stat in pokemon_data["stats"]],
            "stats_no": [stat['base_stat'] for stat in pokemon_data["stats"]]
        }
        results = str(pokemon_info)
        return results
    else:
        return None

pokemon_questions = dspy.ReAct(pokemon_agent, tools=[pokemon_lookup], max_iters=1) #The DSPy module expects a DSPy.Signature and Tools or the python functions we would like it to use
result = pokemon_questions(pokemon_question="What is Sinistcha", conversation_history=None) #Remember, the signature expects two inputs. If I don't provide them, we will ses an error 
print(f"Notice how the final output contains the inputs and outputs. You can access the programatically as shown below:\n\nThe full output:\n {result}\n\nThe answer itself:\n {result.response}\n\nThe stored conversatin history{result.history}\n\nWhat agent the LLM decided to go to next:\n {result.next_agent}") 

# COMMAND ----------

# MAGIC %md
# MAGIC If you want to see the entire raw prompt of what DSPy generated, you can use lm.history or swap out the "lm" part with the LLM variable you set at the beginning. Note, you have to change cache=True for this to be saved. 
# MAGIC
# MAGIC You can also use MLflow traces to track this 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Step 3: Put it altogether 
# MAGIC
# MAGIC As you could probably guess from the name, modules help modularize the LLM call. This is critical for helping build our Agents. Once you have a few created, you can piece them together as submodules within one big module to have them interact with each other. 
# MAGIC
# MAGIC Below is a big implementation of a single module called multi_agent_module. It's designed to work with a Gradio ChatbotInterface and show what each agent is responding with in the interface itself. 
# MAGIC
# MAGIC For custom modules, you must have at least an init function and a forward function. The init function is where you would define your submodules to be used in the forward function. When the module is called the same way as the examples above, it will first go to the forward function by default. 
# MAGIC
# MAGIC You will also notice that, while we define the tools within the module, you can put said function/tool anywhere else (UC functions for example)
# MAGIC
# MAGIC In a real life use case, this implementation would be much simpler, connecting the agents together more efficiently. However, to visualize what exactly is happening between the Agents, we are using a pretty Gradio UI to see it happen. 
# MAGIC
# MAGIC If you would rather interact with this code in the notebook itself and not through Gradio, skip ahead to cell 29. 

# COMMAND ----------

import dspy 
import requests
import os
from databricks.vector_search.client import VectorSearchClient
import time
import mlflow
from PIL import Image
import base64
from io import BytesIO
import base64
import pandas as pd
import pytesseract
import json

class multi_agent_module(dspy.Module): 
  def __init__(self):
    mlflow.dspy.autolog()
    self.router_agent_class = dspy.Predict(router_agent)
    self.pokemon_agent_class = dspy.ReAct(pokemon_agent, tools=[self.pokemon_lookup], max_iters=1)
    self.databricks_agent_class = dspy.ReAct(databricks_agent, tools=[self.databricks_documentation], max_iters=1)
    self.vision_agent_class = dspy.ReAct(vision_agent, tools=[self.vision_transformer_tool], max_iters=1)
    self.sales_agent_class = dspy.ReAct(sales_agent, tools=[self.execute_order], max_iters=1)
    self.memory = []

  def pokemon_lookup(self, pokemon):
    """use to find more information about specific pokemon"""
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon.lower()}"
    response = requests.get(url)
    if response.status_code == 200:
        pokemon_data = response.json()
        pokemon_info = {
            "name": pokemon_data["name"],
            "height": pokemon_data["height"],
            "weight": pokemon_data["weight"],
            "abilities": [ability["ability"]["name"] for ability in pokemon_data["abilities"]],
            "types": [type_data["type"]["name"] for type_data in pokemon_data["types"]],
            "stats_name": [stat['stat']['name'] for stat in pokemon_data["stats"]],
            "stats_no": [stat['base_stat'] for stat in pokemon_data["stats"]]
        }
        results = str(pokemon_info)
        return results
    else:
        return None

  #This tool uses a Databricks Model Serving endpoint. This endpoint has a computer vision model to do image classification 
  def vision_transformer_tool(self, url, task):
    """Used to classify an image. Use this to answer the user's question about an image"""
    API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    DATABRICKS_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    print(f"The task: {task}")
    print(f"The url: {url}")
    if task == 'ocr':
      model = ocr_model_name
      image_path = url
      image_response = requests.get(image_path)
      image_bytes = image_response.content
        
      encoded_image = base64.b64encode(image_bytes).decode('utf-8')

      input_data = pd.DataFrame({'image': [encoded_image]})

      input_json = input_data.to_json(orient='split')

      payload = {
          "dataframe_split": json.loads(input_json)
      }

      headers = {"Context-Type": "text/json", "Authorization": f"Bearer {API_TOKEN}"}

      response = requests.post(
          url=f"https://{DATABRICKS_URL}/serving-endpoints/{model}/invocations", json=payload, headers=headers
      )

      result2 = response.json()
      print(result2['predictions'])
      return result2['predictions']
    
    if task == 'classification':
      model = beit_model_name
      data = {"inputs": [url]}

      headers = {"Context-Type": "text/json", "Authorization": f"Bearer {API_TOKEN}"}

      response = requests.post(
          url=f"https://{DATABRICKS_URL}/serving-endpoints/{model}/invocations", json=data, headers=headers
      )

      result = response.json()
      return result['predictions'][0]['0']['label']  

  #this is a Databricks Vector Search call to pull Databricks Documentation 
  def databricks_documentation(self, databricks_question):
    """This function needs the User's question. The question is used to pull documentation about Databricks. Use the information to answer the user's question"""
    
    print(f"One of the questions for RAG: {databricks_question}")
    workspace_url = os.environ.get("WORKSPACE_URL")
    sp_client_id = os.environ.get("SP_CLIENT_ID")
    sp_client_secret = os.environ.get("SP_CLIENT_SECRET")

    vsc = VectorSearchClient(
        workspace_url=workspace_url,
        service_principal_client_id=sp_client_id,
        service_principal_client_secret=sp_client_secret,
        disable_notice=True
    )

    # index = vsc.get_index(endpoint_name="one-env-shared-endpoint-5", index_name="db500west.dbdemos_rag_chatbot.databricks_documentation_vs_index")
    index = vsc.get_index(endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME, index_name=vectorSearchIndexName)

    result = index.similarity_search(num_results=3, columns=["content"], query_text=databricks_question)

    return result['result']['data_array'][0][0]

  #Hard Coded for Demo Purposes
  def execute_order(self, product, price: int):
    """Pretend, Demo to demonstrate the tool calling"""
    print("\n\n=== Order Summary ===")
    print(f"Product: {product}")
    print(f"Price: ${price}")
    print("=================\n")
    confirm = input("Confirm order? y/n: ").strip().lower()
    if confirm == "y":
        print("Order execution successful!")
        return "Success"
    else:
        print("Order cancelled!")
        return "User cancelled order."

  #Hard Coded for Demo Purposes
  def look_up_item(self, search_query):
    """Pretend, Demo to demonstrate the tool calling """
    item_id = "item_sold_1234"
    print("Found item:", item_id)
    return item_id
  
  def process_agent_response(self, agent_name, response, current_question, messages):
    """Helper method to process agent responses and determine next steps"""
    messages.append(response.history)
    print(f"{agent_name} Response: {response.response}\n")
    
    next_agent = response.next_agent
    return next_agent, messages, response.response
  
  def handle_question(self, question, messages, next_agent):
    """Process a single question through the appropriate agents"""
    # start_time = time.time()
    current_question = question
    # next_agent = next_agent
    # response_text = "" 
    
    while True:
        if not next_agent:
          print("Starting at Router Agent")
          with dspy.context(lm=router_model):
            print(f"Current Model: {router_model_name}\n")
            next_agent_determine = self.router_agent_class(
                question=current_question, 
                conversation_history=messages
            )
          next_agent = next_agent_determine.next_agent
          yield "RouterAgent", f"Transfering to the next agent: {next_agent}", False, next_agent
          continue

        if next_agent == 'pokemon_expert':
          with dspy.context(lm=pokemon_model):
            print(f"Current Model: {pokemon_model_name}\n")
            response = self.pokemon_agent_class(
                pokemon_question=current_question,
                conversation_history=messages
            )
          next_agent, messages, response_text = self.process_agent_response('Pokemon Agent', response, current_question, messages)
          is_final = next_agent == 'pokemon_expert'
          yield "Pokemon Agent", response_text, is_final, next_agent
          if is_final:
              break
          continue

        elif next_agent == 'databricks_expert':
          with dspy.context(lm=databricks_model):
            print(f"Current Model: {databricks_model_name}\n")
            response = self.databricks_agent_class(
                databricks_question=current_question,
                conversation_history=messages
            )
          next_agent, messages, response_text = self.process_agent_response('Databricks Agent', response, current_question, messages)
          is_final = next_agent == 'databricks_expert'
          yield "Databricks Agent", response_text, is_final, next_agent
          if is_final:
              break
          continue

        elif next_agent == 'image_expert':
          with dspy.context(lm=vision_model):
            print(f"Current Model: {vision_model_name}\n")
            response = self.vision_agent_class(
                user_question=current_question,
                conversation_history=messages
            )
          next_agent, messages, response_text = self.process_agent_response('Vision Agent', response, current_question, messages)
          is_final = next_agent == 'image_expert'
          yield "Vision Agent", response_text, is_final, next_agent
          if is_final:
              break
          continue

        elif next_agent == 'sales_expert':
          with dspy.context(lm=sales_model):
            print(f"Current Model: {sales_model_name}\n")
            response = self.sales_agent_class(
                user_question=current_question,
                conversation_history=messages
            )
          next_agent, messages, response_text = self.process_agent_response('Sales Agent', response, current_question, messages)
          is_final = next_agent == 'sales_expert'
          yield "Sales Agent", response_text, is_final, next_agent
          if is_final:
              break
          continue

        elif next_agent == 'router_agent':
          if question.lower() == 'end conversation':
            break
          next_agent_determine = self.router_agent_class(
              question=current_question,
              conversation_history=messages
          )
          next_agent = next_agent_determine.next_agent
          yield "Router", f"Re-routing query to {next_agent}", False, next_agent
          continue

  def forward(self, question, messages, next_agent):
    """Main interaction loop"""
    next_agent, messages, response_text = self.handle_question(question=question, messages=messages, next_agent=next_agent)
    return next_agent, messages, response_text

# COMMAND ----------

# MAGIC %md
# MAGIC The code below is specifically so that we can use it with Gradio and have messages appear correctly in the interface.

# COMMAND ----------

# DBTITLE 1,Gradio specific code
class MultiAgentChatbot:
    def __init__(self):
        self.agent_module = multi_agent_module()
        self.messages = []
        self.next_agent = ""

    def chat(self, message, history):
        """
        Gradio chat interface handler
        """
        mlflow.dspy.autolog()
        try:
            self.messages.append({"role": "user", "content": message})
            
            # Process the message through the agent system
            responses = []
            for response in self.agent_module.handle_question(
                message, 
                self.messages, 
                next_agent=self.next_agent
            ):
                agent_name, response_text, is_final, next_agent = response
                if agent_name == "Router":
                    responses.append(f"🔄 Router: Directing to {next_agent}...")
                else:
                    responses.append(f"🤖 {agent_name}: {response_text}")
                
                if is_final:
                    self.next_agent = next_agent
            
            # Join all responses with newlines
            full_response = "\n\n".join(responses)
            return full_response
            
        except Exception as e:
            print(f"Error in chat processing: {str(e)}")
            return f"I encountered an error while processing your message. Please try again."

# COMMAND ----------

# DBTITLE 1,Gradio Specific Code
import gradio as gr
import mlflow
from IPython.display import Markdown

def create_demo():
  chatbot = MultiAgentChatbot()
  mlflow.set_registry_uri("databricks-uc")
  current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
  
  mlflow.set_experiment(f"/Users/{current_user}/dspy_multi_agent_chatbot_demo_austin_choi")
    
  # Create the Gradio interface with specific styling
  with gr.Blocks(fill_height=True) as demo:
    gr.Markdown("# <img style='float: left' width='50px' src='https://cdn.freelogovectors.net/wp-content/uploads/2023/04/databrickslogo-freelogovectors.net_.png'> Databricks + DSPy Multi-Agent Chatbot")
    gr.Markdown("A chatbot designed with DSPy and hosted with Databricks!\n\n**Why DSPy?** DSPy helped me quickly test and iterate over my individual agents. I could spend more time **programming** my GenAI app instead of writing huge blocks of text for prompts. Most importantly, it helped modularize each step these agents took.\n\n**Why Databricks?** Databricks provided the end to end infrastructure I needed to both power this chatbot but also evaluate, test and log the activity of each Agent. You can quickly lose track of what the Agents are doing and you must have a way to quickly isolate root causes when they go haywire.")
    gr.Markdown(f"This Chatbot has 5 agents that do the following:\n1. **Pokemon Agent**: This makes an external API call via PokeAPI to get up to date, accurate information about a pokemon (trust me, ChatGPT and Claude don't know the pokemon Sinistcha, try it)\n2. **Vision Agent**: This agent takes a user's request with an image url, understands what the user would like to do with the image and picks a Computer Vision Model to process the Image. Both of these computer vision models are hosted on Databricks Model Serving. It currently decides between a classification vision transformer (beIT) and the classic OCR Tesseract model\n3. **Sales Agent**: This agent mimics a salesman trying to sell you products.\n4. **Databricks Agent**: This agent uses Databrick's VectorSearchEndpoint to do classic RAG on a knowledge base of Databricks documentation. If the question is too generic, it will also create subquestions to pull more information and do further analysis\n5. **Router Agent**: This agent handles any completely unknown request and tries to send the request to the right agent\n\nAll the agents can transfer your request to another agent if they deem your question to be irrelevant to their specific task!\n\nCheck out the write up here to learn more about the technical implementation: https://medium.com/@austinchoi/enable-multi-agents-classic-ml-using-databricks-and-dspy-918f78c16e3a")
    chatbot_interface = gr.ChatInterface(
        fn=chatbot.chat,
        fill_height=True,
        examples=[
            ["Tell me about Sinistcha"],
            ["How do I use Databricks ML Flow?"],
            ["Can you analyze this image?"],
            ["I want to place an order"]
        ],
        theme='soft'
    )
  
  return demo

# COMMAND ----------

demo = create_demo()
demo.launch(share=True)
# Use these examples below to try the vision agent out
# Example for Vision Agent: Classify this image: https://d323w7klwy72q3.cloudfront.net/i/a/2025/20250115ve/FT_EG2300.JPG
# Example for Vision Agent: Extract text: https://variety.com/wp-content/uploads/2017/09/longer-tweets.jpg

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run it! 
# MAGIC
# MAGIC We use MLflow to track and log our LLM calls

# COMMAND ----------

databricks_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

import dspy 
import requests
import os
from databricks.vector_search.client import VectorSearchClient
import time

class multi_agent_module(dspy.Module):
  def __init__(self):
    self.router_agent_class = dspy.Predict(router_agent)
    self.pokemon_agent_class = dspy.ReAct(pokemon_agent, tools=[self.pokemon_lookup], max_iters=1)
    self.databricks_agent_class = dspy.ReAct(databricks_agent, tools=[self.databricks_documentation], max_iters=1)
    self.vision_agent_class = dspy.ReAct(vision_agent, tools=[self.vision_transformer_tool], max_iters=1)
    self.sales_agent_class = dspy.ReAct(sales_agent, tools=[self.execute_order], max_iters=1)
    self.memory = []

  def pokemon_lookup(self, pokemon):
    """use to find more information about specific pokemon"""
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon.lower()}"
    response = requests.get(url)
    if response.status_code == 200:
        pokemon_data = response.json()
        pokemon_info = {
            "name": pokemon_data["name"],
            "height": pokemon_data["height"],
            "weight": pokemon_data["weight"],
            "abilities": [ability["ability"]["name"] for ability in pokemon_data["abilities"]],
            "types": [type_data["type"]["name"] for type_data in pokemon_data["types"]],
            "stats_name": [stat['stat']['name'] for stat in pokemon_data["stats"]],
            "stats_no": [stat['base_stat'] for stat in pokemon_data["stats"]]
        }
        results = str(pokemon_info)
        return results
    else:
        return None

  #This tool uses a Databricks Model Serving endpoint. This endpoint has a computer vision model to do image classification 
  def vision_transformer_tool(self, url, task):
    """Used to classify an image. Use this to answer the user's question about an image"""
    API_TOKEN = api_token
    DATABRICKS_URL = databricks_url
    print(f"The task: {task}")
    print(f"The url: {url}")
    if task == 'ocr':
      model = ocr_model_name
      image_path = url
      image_response = requests.get(image_path)
      image_bytes = image_response.content
        
      encoded_image = base64.b64encode(image_bytes).decode('utf-8')

      input_data = pd.DataFrame({'image': [encoded_image]})

      input_json = input_data.to_json(orient='split')

      payload = {
          "dataframe_split": json.loads(input_json)
      }

      headers = {"Context-Type": "text/json", "Authorization": f"Bearer {API_TOKEN}"}

      response = requests.post(
          url=f"https://{DATABRICKS_URL}/serving-endpoints/{model}/invocations", json=payload, headers=headers
      )

      result2 = response.json()
      print(result2['predictions'])
      return result2['predictions']
    
    if task == 'classification':
      model = beit_model_name
      data = {"inputs": [url]}

      headers = {"Context-Type": "text/json", "Authorization": f"Bearer {API_TOKEN}"}

      response = requests.post(
          url=f"https://{DATABRICKS_URL}/serving-endpoints/{model}/invocations", json=data, headers=headers
      )

      result = response.json()
      return result['predictions'][0]['0']['label']  

  #this is a Databricks Vector Search call to pull Databricks Documentation 
  def databricks_documentation(self, databricks_question):
    """This function needs the User's question. The question is used to pull documentation about Databricks. Use the information to answer the user's question"""
    
    print(f"One of the questions for RAG: {databricks_question}")
    workspace_url = os.environ.get("WORKSPACE_URL")
    sp_client_id = os.environ.get("SP_CLIENT_ID")
    sp_client_secret = os.environ.get("SP_CLIENT_SECRET")

    vsc = VectorSearchClient(
        workspace_url=workspace_url,
        service_principal_client_id=sp_client_id,
        service_principal_client_secret=sp_client_secret,
        disable_notice=True
    )

    # index = vsc.get_index(endpoint_name="one-env-shared-endpoint-5", index_name="db500west.dbdemos_rag_chatbot.databricks_documentation_vs_index")
    index = vsc.get_index(endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME, index_name=vectorSearchIndexName)

    result = index.similarity_search(num_results=3, columns=["content"], query_text=databricks_question)

    return result['result']['data_array'][0][0]

  #Hard Coded for Demo Purposes
  def execute_order(self, product, price: int):
    """Price should be in USD."""
    print("\n\n=== Order Summary ===")
    print(f"Product: {product}")
    print(f"Price: ${price}")
    print("=================\n")
    confirm = input("Confirm order? y/n: ").strip().lower()
    if confirm == "y":
        print("Order execution successful!")
        return "Success"
    else:
        print("Order cancelled!")
        return "User cancelled order."

  #Hard Coded for Demo Purposes
  def look_up_item(self, search_query):
    """Use to find item ID.
    Search query can be a description or keywords."""
    item_id = "item_132612938"
    print("Found item:", item_id)
    return item_id
  
  def process_agent_response(self, agent_name, response, current_question, messages):
    """Helper method to process agent responses and determine next steps"""
    messages.append(response.history)
    print(f"{agent_name} Response: {response.response}\n")
    
    next_agent = response.next_agent
    return next_agent, messages
  
  def handle_question(self, question, messages, next_agent):
    """Process a single question through the appropriate agents"""
    start_time = time.time()
    current_question = question
    next_agent = next_agent
    
    while True:
        if not next_agent:
            print("Starting at Router Agent")
            with dspy.context(lm=router_model):
              print(f"Current Model: {router_model_name}\n")
              next_agent_determine = self.router_agent_class(
                  question=current_question, 
                  conversation_history=messages
              )
            next_agent = next_agent_determine.next_agent
            print(f"Transfering to the next agent: {next_agent}")
            continue

        if next_agent == 'pokemon_expert':
            with dspy.context(lm=pokemon_model):
              print(f"Current Model: {pokemon_model_name}\n")
              response = self.pokemon_agent_class(
                  pokemon_question=current_question,
                  conversation_history=messages
              )
            next_agent, messages = self.process_agent_response('Pokemon Agent', response, current_question, messages)
            if next_agent == 'pokemon_expert':
                break
            else:
              print(f"Transfering to the next agent: {next_agent}")
              continue 

        elif next_agent == 'databricks_expert':
            with dspy.context(lm=databricks_model):
              print(f"Current Model: {databricks_model_name}\n")
              response = self.databricks_agent_class(
                  databricks_question=current_question,
                  conversation_history=messages
              )
            next_agent, messages = self.process_agent_response('Databricks Agent', response, current_question, messages)
            if next_agent == 'databricks_expert':
                break
            else:
              print(f"Transfering to the next agent: {next_agent}")
              continue

        elif next_agent == 'image_expert':
            with dspy.context(lm=vision_model):
              print(f"Current Model: {vision_model_name}\n")
              response = self.vision_agent_class(
                  user_question=current_question,
                  conversation_history=messages
              )
            next_agent, messages = self.process_agent_response('Vision Agent', response, current_question, messages)
            if next_agent == 'image_expert':
              break 
            else:
              print(f"Transfering to the next agent: {next_agent}")
              continue

        elif next_agent == 'sales_expert':
            with dspy.context(lm=sales_model):
              print(f"Current Model: {sales_model_name}\n")
              response = self.sales_agent_class(
                  user_question=current_question,
                  conversation_history=messages
              )
            next_agent, messages = self.process_agent_response('Sales Agent', response, current_question, messages)
            if next_agent == 'sales_expert':
                break
            else:
              print(f"Transfering to the next agent: {next_agent}")
              continue

        elif next_agent == 'router_agent':
          if question.lower() == 'end conversation':
            break
          next_agent_determine = self.router_agent_class(
              question=current_question,
              conversation_history=messages
          )
          next_agent = next_agent_determine.next_agent
          print(f"Transfering to the next agent: {next_agent}")
          continue

    end_time = time.time()
    print(f"Processing time: {end_time-start_time} seconds")
    return next_agent, messages

  def forward(self):
    """Main interaction loop"""
    messages = []
    next_agent = ""
    while True:
        question = input("User: ")
        if question.lower() == 'end conversation':
            print(f"Thanks for chatting with us!")
            break
        messages.append({"role": "user", "content": question})
        next_agent, messages = self.handle_question(question, messages, next_agent=next_agent)
        print(f"Awaiting next user input: ")

# COMMAND ----------

# MAGIC %md
# MAGIC # Use MLflow To Track Your Agent's Behaviors
# MAGIC
# MAGIC mlflow.dspy.autolog() will automatically log what is happening between the agents for you to review later. You can use this information to better evaluate what is happening and which agent is not performing to your needs

# COMMAND ----------

import mlflow
from IPython.display import Markdown
mlflow.set_registry_uri("databricks-uc")
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
experiment_name = "dspy_multi_agent_chatbot_demo_austin_choi"
mlflow.dspy.autolog()
mlflow.set_experiment(f"/Users/{current_user}/{experiment_name}")

multi_agent = multi_agent_module()
result = multi_agent()
# Example for Vision Agent: Classify this image: https://d323w7klwy72q3.cloudfront.net/i/a/2025/20250115ve/FT_EG2300.JPG
# Example for Vision Agent: Extract text: https://variety.com/wp-content/uploads/2017/09/longer-tweets.jpg

# COMMAND ----------

import time
import mlflow
import pandas as pd
from IPython.display import Markdown
from mlflow.models import ModelSignature
from mlflow.types.schema import ColSpec, Schema
from mlflow.types.llm import ChatCompletionRequest, ChatCompletionResponse, ChatChoice, ChatMessage

input_schema = Schema([ColSpec("string")])
output_schema = Schema([ColSpec("string")])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

with mlflow.start_run():
  model_info = mlflow.dspy.log_model(
    dspy_model = multi_agent_module(),
    artifact_path='agent',
    signature=signature,
    registered_model_name='austin_choi_demo_catalog.agents.dspy_multi_turn_chatbot'
  )

# COMMAND ----------

# MAGIC %md
# MAGIC #Deploy your DSPy Agent on Databricks using the Mosaic AI Agent Framework 
# MAGIC
# MAGIC Here you will use Databrick's agent.deploy() function to quickly set up model serving endpoints. This will set up many of our production ready features to help your application become production ready! This will allow you to take advatange of the following: 
# MAGIC
# MAGIC 1. An API endpoint to serve into your user-facing applications 
# MAGIC 2. AI Gateway for endpoint management 
# MAGIC 3. Inference Tables for traffic logging 
# MAGIC 4. Security and Credentials pmanagement 
# MAGIC 5. A review app to immeidately share the app with shareholders to provide feedback 
# MAGIC
# MAGIC More details can be found here: https://docs.databricks.com/en/generative-ai/agent-framework/deploy-agent.html

# COMMAND ----------

# DBTITLE 1,Refactor the Code to remove the Loop
import dspy 
import requests
import os
from databricks.vector_search.client import VectorSearchClient
import time

class multi_agent_module(dspy.Module):
  def __init__(self):
    self.router_agent_class = dspy.Predict(router_agent)
    self.pokemon_agent_class = dspy.ReAct(pokemon_agent, tools=[self.pokemon_lookup], max_iters=1)
    self.databricks_agent_class = dspy.ReAct(databricks_agent, tools=[self.databricks_documentation], max_iters=1)
    self.vision_agent_class = dspy.ReAct(vision_agent, tools=[self.vision_transformer_tool], max_iters=1)
    self.sales_agent_class = dspy.ReAct(sales_agent, tools=[self.execute_order], max_iters=1)
    self.memory = []

  def pokemon_lookup(self, pokemon):
    """use to find more information about specific pokemon"""
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon.lower()}"
    response = requests.get(url)
    if response.status_code == 200:
        pokemon_data = response.json()
        pokemon_info = {
            "name": pokemon_data["name"],
            "height": pokemon_data["height"],
            "weight": pokemon_data["weight"],
            "abilities": [ability["ability"]["name"] for ability in pokemon_data["abilities"]],
            "types": [type_data["type"]["name"] for type_data in pokemon_data["types"]],
            "stats_name": [stat['stat']['name'] for stat in pokemon_data["stats"]],
            "stats_no": [stat['base_stat'] for stat in pokemon_data["stats"]]
        }
        results = str(pokemon_info)
        return results
    else:
        return None

  #This tool uses a Databricks Model Serving endpoint. This endpoint has a computer vision model to do image classification 
  def vision_transformer_tool(self, url, task):
    """Used to classify an image. Use this to answer the user's question about an image"""
    API_TOKEN = api_token
    DATABRICKS_URL = databricks_url
    print(f"The task: {task}")
    print(f"The url: {url}")
    if task == 'ocr':
      model = ocr_model_name
      image_path = url
      image_response = requests.get(image_path)
      image_bytes = image_response.content
        
      encoded_image = base64.b64encode(image_bytes).decode('utf-8')

      input_data = pd.DataFrame({'image': [encoded_image]})

      input_json = input_data.to_json(orient='split')

      payload = {
          "dataframe_split": json.loads(input_json)
      }

      headers = {"Context-Type": "text/json", "Authorization": f"Bearer {API_TOKEN}"}

      response = requests.post(
          url=f"https://{DATABRICKS_URL}/serving-endpoints/{model}/invocations", json=payload, headers=headers
      )

      result2 = response.json()
      print(result2['predictions'])
      return result2['predictions']
    
    if task == 'classification':
      model = beit_model_name
      data = {"inputs": [url]}

      headers = {"Context-Type": "text/json", "Authorization": f"Bearer {API_TOKEN}"}

      response = requests.post(
          url=f"https://{DATABRICKS_URL}/serving-endpoints/{model}/invocations", json=data, headers=headers
      )

      result = response.json()
      return result['predictions'][0]['0']['label']  

  #this is a Databricks Vector Search call to pull Databricks Documentation 
  def databricks_documentation(self, databricks_question):
    """This function needs the User's question. The question is used to pull documentation about Databricks. Use the information to answer the user's question"""
    
    print(f"One of the questions for RAG: {databricks_question}")
    workspace_url = os.environ.get("WORKSPACE_URL")
    sp_client_id = os.environ.get("SP_CLIENT_ID")
    sp_client_secret = os.environ.get("SP_CLIENT_SECRET")

    vsc = VectorSearchClient(
        workspace_url=workspace_url,
        service_principal_client_id=sp_client_id,
        service_principal_client_secret=sp_client_secret,
        disable_notice=True
    )

    # index = vsc.get_index(endpoint_name="one-env-shared-endpoint-5", index_name="db500west.dbdemos_rag_chatbot.databricks_documentation_vs_index")
    index = vsc.get_index(endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME, index_name=vectorSearchIndexName)

    result = index.similarity_search(num_results=3, columns=["content"], query_text=databricks_question)

    return result['result']['data_array'][0][0]

  #Hard Coded for Demo Purposes
  def execute_order(self, product, price: int):
    """Price should be in USD."""
    print("\n\n=== Order Summary ===")
    print(f"Product: {product}")
    print(f"Price: ${price}")
    print("=================\n")
    confirm = input("Confirm order? y/n: ").strip().lower()
    if confirm == "y":
        print("Order execution successful!")
        return "Success"
    else:
        print("Order cancelled!")
        return "User cancelled order."

  #Hard Coded for Demo Purposes
  def look_up_item(self, search_query):
    """Use to find item ID.
    Search query can be a description or keywords."""
    item_id = "item_132612938"
    print("Found item:", item_id)
    return item_id
  
  def process_agent_response(self, agent_name, response, current_question, messages):
    """Helper method to process agent responses and determine next steps"""
    messages.append(response.history)
    print(f"{agent_name} Response: {response.response}\n")
    
    next_agent = response.next_agent
    return next_agent, messages
  
  def handle_question(self, question, messages, next_agent):
    """Process a single question through the appropriate agents"""
    start_time = time.time()
    current_question = question
    next_agent = next_agent
    
    while True:
        if not next_agent:
            print("Starting at Router Agent")
            with dspy.context(lm=router_model):
              print(f"Current Model: {router_model_name}\n")
              next_agent_determine = self.router_agent_class(
                  question=current_question, 
                  conversation_history=messages
              )
            next_agent = next_agent_determine.next_agent
            print(f"Transfering to the next agent: {next_agent}")
            continue

        if next_agent == 'pokemon_expert':
            with dspy.context(lm=pokemon_model):
              print(f"Current Model: {pokemon_model_name}\n")
              response = self.pokemon_agent_class(
                  pokemon_question=current_question,
                  conversation_history=messages
              )
            next_agent, messages = self.process_agent_response('Pokemon Agent', response, current_question, messages)
            if next_agent == 'pokemon_expert':
                break
            else:
              print(f"Transfering to the next agent: {next_agent}")
              continue 

        elif next_agent == 'databricks_expert':
            with dspy.context(lm=databricks_model):
              print(f"Current Model: {databricks_model_name}\n")
              response = self.databricks_agent_class(
                  databricks_question=current_question,
                  conversation_history=messages
              )
            next_agent, messages = self.process_agent_response('Databricks Agent', response, current_question, messages)
            if next_agent == 'databricks_expert':
                break
            else:
              print(f"Transfering to the next agent: {next_agent}")
              continue

        elif next_agent == 'image_expert':
            with dspy.context(lm=vision_model):
              print(f"Current Model: {vision_model_name}\n")
              response = self.vision_agent_class(
                  user_question=current_question,
                  conversation_history=messages
              )
            next_agent, messages = self.process_agent_response('Vision Agent', response, current_question, messages)
            if next_agent == 'image_expert':
              break 
            else:
              print(f"Transfering to the next agent: {next_agent}")
              continue

        elif next_agent == 'sales_expert':
            with dspy.context(lm=sales_model):
              print(f"Current Model: {sales_model_name}\n")
              response = self.sales_agent_class(
                  user_question=current_question,
                  conversation_history=messages
              )
            next_agent, messages = self.process_agent_response('Sales Agent', response, current_question, messages)
            if next_agent == 'sales_expert':
                break
            else:
              print(f"Transfering to the next agent: {next_agent}")
              continue

        elif next_agent == 'router_agent':
          if question.lower() == 'end conversation':
            break
          next_agent_determine = self.router_agent_class(
              question=current_question,
              conversation_history=messages
          )
          next_agent = next_agent_determine.next_agent
          print(f"Transfering to the next agent: {next_agent}")
          continue

    end_time = time.time()
    print(f"Processing time: {end_time-start_time} seconds")
    return next_agent, messages

  def forward(self, messages, next_agent, question):
    """Main interaction loop"""
    messages = []
    next_agent = next_agent
    # question = input("User: ")
    messages.append({"role": "user", "content": question})
    next_agent, messages = self.handle_question(question, messages, next_agent=next_agent)
    # print(f"Awaiting next user input: ")
    return next_agent, messages

# COMMAND ----------

# MAGIC %md
# MAGIC ###MLflow Chat Model 
# MAGIC
# MAGIC agent.deploy requires you to use MLflow chatmodel to comply with the chat completions requests set by agent.Deploy. It uses MLflow LLM Types to maintain the LLM structure and handle your inputs and outputs and streaming.
# MAGIC
# MAGIC We create a class using MLflow Chat Model to return a ChatCompletionResponse. 

# COMMAND ----------

from dataclasses import dataclass
from typing import Optional, Dict, List, Generator
from mlflow.pyfunc import ChatModel
from mlflow.types.llm import (
    # Non-streaming helper classes
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatCompletionChunk,
    ChatMessage,
    ChatChoice,
    ChatParams,
    # Helper classes for streaming agent output
    ChatChoiceDelta,
    ChatChunkChoice,
)

multi_agent = multi_agent_module()

class DSPyAgent(ChatModel):
    def __init__(self):
        self.multi_agent = multi_agent_module()
    
    def _prepare_messages(self, messages: List[ChatMessage]):
        return {"messages": [m.to_dict() for m in messages]}
      
    def predict(self, context, messages: list[ChatMessage], params=None) -> ChatCompletionResponse:
        question = messages[-1].content
        print(question)
        print(messages)
        next_agent, response = self.multi_agent(messages=messages, next_agent="", question=question)
        response_message = ChatMessage(
            role="assistant",
            content=(
                f"{response}"
            )
        )
        return ChatCompletionResponse(
            choices=[ChatChoice(message=response_message)]
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test your Agent!

# COMMAND ----------

agent = DSPyAgent()
model_input = ChatCompletionRequest(
    messages=[ChatMessage(role="user", content="What is Databricks?")]
)
response = agent.predict(context=None, messages=model_input.messages)
print(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Log Model to Unity Catalog using Pyfunc

# COMMAND ----------

import mlflow
with mlflow.start_run():
  model_info = mlflow.pyfunc.log_model(
    python_model = DSPyAgent(),
    artifact_path = 'model',
    input_example={
            "messages": [{"role": "user", "content": "What is Sinistcha?"}]
        },
    registered_model_name='austin_choi_demo_catalog.agents.dspy_multi_turn_chatbot_agent_deploy'
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Deploy! 
# MAGIC
# MAGIC The Deploy function looks for: 
# MAGIC 1. The registered model name which is catalog.schema.model_name 
# MAGIC 2. The version number you would like to use. You will likely have multiple versions of the model based on your testing 

# COMMAND ----------


from databricks.agents import deploy
from mlflow.utils import databricks_utils as du
from mlflow.types.llm import ChatCompletionRequest, ChatCompletionResponse, ChatChoice, ChatMessage

deployment = deploy(model_name='austin_choi_demo_catalog.agents.dspy_multi_turn_chatbot_agent_deploy', model_version=model_info.registered_model_version)

# query_endpoint is the URL that can be used to make queries to the app
deployment.query_endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Copy deployment.rag_app_url to browser and start interacting with your RAG application. 
# MAGIC
# MAGIC Or click the URLs in the cell above's output

# COMMAND ----------

# DBTITLE 1,This will not work until app is deployed (view status)
deployment.rag_app_url

# COMMAND ----------

# MAGIC %md
# MAGIC #What's Next? 
# MAGIC
# MAGIC There is a whole side of evaluation and optimizations here that Databricks and DSPy provides. We can improve the performance of this module by using DSPy's optimization capabilities and it is essentially like training a ML model! 
# MAGIC
# MAGIC We will likely use the following: 
# MAGIC 1. DSPy Optimizers 
# MAGIC 2. Databricks Synthetic Data generation 
# MAGIC 3. Databricks Mosaic AI Agent Framework for Evaluations