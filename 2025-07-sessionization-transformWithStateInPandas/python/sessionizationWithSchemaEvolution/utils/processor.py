import pandas as pd
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DoubleType, BooleanType
from typing import Iterator

class SessionizerV1(StatefulProcessor):
    """
    V1 Processor - ENHANCED with debug output to guarantee results
    
    This processor handles user session tracking by:
    - Maintaining session state across streaming events
    - Counting events and accumulating revenue per session
    - Ending sessions when terminal events occur (purchase, logout)
    - Outputting completed session summaries
    """
    
    def init(self, handle: StatefulProcessorHandle) -> None:
        """
        Initialize the V1 processor with state schema and configuration.
        
        Args:
            handle: StatefulProcessorHandle for managing state operations
        """
        print("[V1 INIT] ========== INITIALIZING V1 PROCESSOR ==========")
        
        # Define V1 State Schema - basic session tracking fields
        state_schema = StructType([
            StructField("session_id", StringType(), True),      # Unique session identifier
            StructField("user_id", StringType(), True),         # User associated with session
            StructField("event_count", IntegerType(), True),    # Number of events in session
            StructField("total_revenue", DoubleType(), True),   # Cumulative revenue for session
            StructField("session_start", TimestampType(), True) # When the session began
        ])
        
        # Initialize state store with the defined schema
        self.session_state = handle.getValueState("session_state", state_schema)
        
        # Define events that trigger session completion
        self.terminal_events = {'purchase', 'logout'}
        
        print(f"[V1 INIT] Terminal events: {self.terminal_events}")
        print("[V1 INIT] ========== V1 PROCESSOR READY ==========")

    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timer_values) -> Iterator[pd.DataFrame]:
        """
        Process incoming streaming data for a specific session key.
        
        Args:
            key: Session identifier (usually session_id)
            rows: Iterator of pandas DataFrames containing event data
            timer_values: Timer information (not used in this implementation)
            
        Yields:
            pd.DataFrame: Completed session summaries when terminal events occur
        """
        # Extract session ID from key (handle both tuple and string formats)
        session_id = key[0] if isinstance(key, tuple) else str(key)
        print(f"\n[V1 BATCH] ========== PROCESSING BATCH FOR {session_id} ==========")
        
        # Process each batch of events for this session
        for batch in rows:
            # Skip empty batches
            if batch.empty:
                print(f"[V1 BATCH] Empty batch for {session_id}")
                continue
                
            print(f"[V1 BATCH] Batch size: {len(batch)} events for {session_id}")
            results = []  # Store completed sessions for output
            
            # Process each event in the batch
            for idx, event in batch.iterrows():
                try:
                    # Extract event data with type conversion
                    event_type = str(event['event_type'])
                    user_id = str(event['user_id'])
                    revenue = float(event['revenue'])
                    timestamp = pd.Timestamp(event['event_timestamp'])
                    
                    print(f"[V1 EVENT] Processing {session_id}: {event_type} (revenue: {revenue})")
                    
                    # Retrieve current session state from state store
                    current_state = self.session_state.get()
                    
                    if current_state is None:
                        # Create new session state for first event
                        state = (session_id, user_id, 1, revenue, timestamp)
                        print(f"[V1 NEW] Created session {session_id} with first event {event_type}")
                    else:
                        # Update existing session state by incrementing counts
                        state = (
                            current_state[0],              # session_id (unchanged)
                            current_state[1],              # user_id (unchanged)
                            current_state[2] + 1,          # event_count + 1
                            current_state[3] + revenue,    # total_revenue + current revenue
                            current_state[4]               # session_start (unchanged)
                        )
                        print(f"[V1 UPDATE] Updated {session_id}: count={state[2]}, revenue={state[3]}")
                    
                    # Check if this event should terminate the session
                    is_terminal = event_type in self.terminal_events
                    print(f"[V1 CHECK] Event {event_type} terminal? {is_terminal}")
                    
                    if is_terminal:
                        # Session completed - create output record
                        result = {
                            'session_id': state[0],
                            'user_id': state[1], 
                            'event_count': int(state[2]),
                            'total_revenue': float(state[3]),
                            'session_start': state[4],
                            'schema_version': 'v1'  # Mark as V1 output
                        }
                        results.append(result)
                        print(f"[V1 COMPLETE] SESSION COMPLETED: {session_id} with {state[2]} events, revenue: {state[3]}")
                        
                        # Clear state since session is complete
                        self.session_state.clear()
                        print(f"[V1 COMPLETE] State cleared for {session_id}")
                    else:
                        # Session continues - update state store with new values
                        self.session_state.update(state)
                        print(f"[V1 CONTINUE] State updated for {session_id}, waiting for terminal event")
                        
                except Exception as e:
                    # Log any processing errors and continue
                    print(f"[V1 ERROR] Error processing event: {e}")
                    import traceback
                    traceback.print_exc()
            
            print(f"[V1 BATCH] Batch complete. Results: {len(results)} sessions completed")
            
            # Output completed sessions or empty DataFrame with correct schema
            if results:
                result_df = pd.DataFrame(results)
                print(f"[V1 OUTPUT] OUTPUTTING {len(results)} COMPLETED SESSIONS:")
                for i, row in result_df.iterrows():
                    print(f"[V1 OUTPUT]   {row['session_id']}: {row['event_count']} events, ${row['total_revenue']}")
                yield result_df
            else:
                # Return empty DataFrame with correct schema for downstream compatibility
                empty_df = pd.DataFrame(columns=['session_id', 'user_id', 'event_count', 'total_revenue', 'session_start', 'schema_version'])
                print(f"[V1 OUTPUT] No completed sessions in this batch")
                yield empty_df

    def close(self) -> None:
        """
        Clean up resources when processor is shut down.
        """
        print("[V1 CLOSE] ========== V1 PROCESSOR CLOSED ==========")
        pass


class SessionizerV2(StatefulProcessor):
    """
    V2 Processor - Reads and evolves V1 state from shared checkpoint
    
    Key fix: Uses identical state variable name and compatible schema initialization
    to ensure V2 can read existing V1 state from the shared checkpoint.
    """
    
    def init(self, handle: StatefulProcessorHandle) -> None:
        """
        Initialize V2 processor to read existing V1 state.
        
        Critical: Must use same state variable name as V1 processor
        """
        print("[V2 INIT] ========== INITIALIZING V2 WITH STATE SHARING ==========")
        
        # CRITICAL FIX: Use evolved schema but ensure compatibility with V1 state
        # When V1 state is read, new fields will be None, existing fields preserved
        state_schema = StructType([
            StructField("session_id", StringType(), True),      # Same as V1
            StructField("user_id", StringType(), True),         # Same as V1
            StructField("event_count", LongType(), True),       # TYPE WIDENING: Int->Long (compatible)
            StructField("total_revenue", DoubleType(), True),   # Same as V1
            StructField("session_start", TimestampType(), True), # Same as V1
            StructField("device_type", StringType(), True),     # NEW FIELD (will be None for V1 state)
            StructField("page_category", StringType(), True)    # NEW FIELD (will be None for V1 state)
        ])
        
        # CRITICAL: Must use identical state variable name as V1 processor
        # This is what enables state sharing between V1 and V2
        self.session_state = handle.getValueState("session_state", state_schema)
        
        # Same terminal events as V1
        self.terminal_events = {'purchase', 'logout'}
        
        print("[V2 INIT] V2 processor ready to read V1 state and evolve schemas")

    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timer_values) -> Iterator[pd.DataFrame]:
        """
        Process events with enhanced state evolution detection.
        """
        session_id = key[0] if isinstance(key, tuple) else str(key)
        print(f"[V2] ========== PROCESSING {session_id} ==========")
        
        for batch in rows:
            if batch.empty:
                continue
                
            results = []
            
            for _, event in batch.iterrows():
                try:
                    # Extract event data
                    event_type = str(event['event_type'])
                    user_id = str(event['user_id'])
                    revenue = float(event['revenue'])
                    timestamp = pd.Timestamp(event['event_timestamp'])
                    
                    # Handle V2 fields (may be None for some events)
                    device_type = str(event.get('device_type', 'unknown')) if event.get('device_type') is not None else 'unknown'
                    page_category = str(event.get('page_category', 'unknown')) if event.get('page_category') is not None else 'unknown'
                    
                    print(f"[V2] Event: {event_type}, Device: {device_type}, Session: {session_id}")
                    
                    # CRITICAL: Get existing state (may be V1 state from previous processor)
                    current_state = self.session_state.get()
                    evolved_from_v1 = False
                    
                    if current_state is None:
                        print(f"[V2] NO EXISTING STATE for {session_id} - creating new V2 session")
                        # Create new V2 session
                        state = (session_id, user_id, 1, revenue, timestamp, device_type, page_category)
                    else:
                        print(f"[V2] FOUND EXISTING STATE for {session_id}: {len(current_state)} fields")
                        print(f"[V2] State content: {current_state}")
                        
                        # ENHANCED EVOLUTION DETECTION
                        # Check if this is V1 state (missing fields 5,6 or they are None)
                        is_v1_state = (len(current_state) <= 5 or 
                                     current_state[5] is None or 
                                     current_state[6] is None)
                        
                        if is_v1_state:
                            print(f"[V2] 🎉 SCHEMA EVOLUTION DETECTED: {session_id} evolving from V1!")
                            evolved_from_v1 = True
                            
                            # Evolve V1 state to V2 format
                            # V1 state: (session_id, user_id, event_count, total_revenue, session_start)
                            # V2 state: (session_id, user_id, event_count, total_revenue, session_start, device_type, page_category)
                            state = (
                                current_state[0],              # session_id
                                current_state[1],              # user_id  
                                current_state[2] + 1,          # event_count + 1 (auto Int->Long conversion)
                                current_state[3] + revenue,    # total_revenue + new revenue
                                current_state[4],              # session_start (unchanged)
                                device_type,                   # NEW: device_type from current event
                                page_category                  # NEW: page_category from current event
                            )
                            print(f"[V2] EVOLVED STATE: {session_id} now has {state[2]} events, device: {state[5]}")
                        else:
                            print(f"[V2] REGULAR V2 UPDATE: {session_id}")
                            # Already V2 format - regular update
                            state = (
                                current_state[0],              # session_id
                                current_state[1],              # user_id
                                current_state[2] + 1,          # event_count + 1
                                current_state[3] + revenue,    # total_revenue + revenue
                                current_state[4],              # session_start
                                current_state[5],              # device_type (keep existing)
                                current_state[6]               # page_category (keep existing)
                            )
                    
                    # Check for session termination
                    is_terminal = event_type in self.terminal_events
                    print(f"[V2] Terminal check: {event_type} -> {is_terminal}")
                    
                    if is_terminal:
                        # Session completed - output result
                        result = {
                            'session_id': state[0],
                            'user_id': state[1],
                            'event_count': state[2],
                            'total_revenue': float(state[3]),
                            'session_start': state[4],
                            'device_type': state[5],
                            'page_category': state[6],
                            'schema_version': 'v2',
                            'evolved_from_v1': evolved_from_v1
                        }
                        results.append(result)
                        
                        evolution_msg = "EVOLVED FROM V1" if evolved_from_v1 else "NEW V2 SESSION"
                        print(f"[V2] COMPLETED: {session_id} - {evolution_msg}")
                        print(f"[V2] FINAL STATE: {state[2]} events, ${state[3]}, {state[5]} device")
                        
                        # Clear state since session is complete
                        self.session_state.clear()
                    else:
                        # Session continues - update state
                        self.session_state.update(state)
                        print(f"[V2] STATE UPDATED: {session_id} continues with {state[2]} events")
                        
                except Exception as e:
                    print(f"[V2] ERROR: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Output results
            if results:
                print(f"[V2] BATCH OUTPUT: {len(results)} completed sessions")
                yield pd.DataFrame(results)
            else:
                print(f"[V2] No completed sessions this batch")
                empty_columns = ['session_id', 'user_id', 'event_count', 'total_revenue', 
                               'session_start', 'device_type', 'page_category', 'schema_version', 'evolved_from_v1']
                yield pd.DataFrame(columns=empty_columns)

    def close(self) -> None:
        """
        Clean up resources when processor is shut down.
        """
        print("[V2] V2 processor closed")
        pass