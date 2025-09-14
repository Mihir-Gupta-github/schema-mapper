"""
LangGraph orchestration for the Schema Mapper & Data Quality Fixer
"""
from typing import Dict, List, Any, TypedDict, Annotated
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
import pandas as pd
from dataclasses import dataclass

from .schema_mapper_agent import SchemaMapperAgent, MappingResult
from .data_cleaner_agent import DataCleanerAgent, CleaningResult
from .llm_fix_agent import LLMFixAgent, FixSuggestion


class DataProcessingState(TypedDict):
    """State for the data processing pipeline"""
    # Input data
    raw_dataframe: pd.DataFrame
    canonical_schema: Dict[str, str]
    
    # Processing results
    column_mappings: List[MappingResult]
    unmapped_columns: List[str]
    cleaned_dataframe: pd.DataFrame
    cleaning_results: List[CleaningResult]
    validation_errors: List[Dict]
    fix_suggestions: List[FixSuggestion]
    
    # User interactions
    manual_mappings: Dict[str, str]
    applied_fixes: List[Dict]
    
    # Final output
    final_dataframe: pd.DataFrame
    processing_summary: Dict[str, Any]


class DataProcessingGraph:
    def __init__(self, canonical_schema: Dict[str, str]):
        self.canonical_schema = canonical_schema
        self.schema_mapper = SchemaMapperAgent(canonical_schema)
        self.data_cleaner = DataCleanerAgent(canonical_schema)
        self.llm_fix_agent = LLMFixAgent(canonical_schema)
        
        # Build the graph
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow"""
        workflow = StateGraph(DataProcessingState)
        
        # Add nodes
        workflow.add_node("map_columns", self._map_columns_node)
        workflow.add_node("clean_data", self._clean_data_node)
        workflow.add_node("validate_data", self._validate_data_node)
        workflow.add_node("generate_fixes", self._generate_fixes_node)
        workflow.add_node("apply_manual_mappings", self._apply_manual_mappings_node)
        workflow.add_node("apply_fixes", self._apply_fixes_node)
        workflow.add_node("finalize_output", self._finalize_output_node)
        
        # Define the flow
        workflow.set_entry_point("map_columns")
        
        workflow.add_edge("map_columns", "clean_data")
        workflow.add_edge("clean_data", "validate_data")
        workflow.add_edge("validate_data", "generate_fixes")
        
        # Conditional edges for user interaction
        workflow.add_conditional_edges(
            "generate_fixes",
            self._should_apply_manual_mappings,
            {
                "manual_mapping": "apply_manual_mappings",
                "apply_fixes": "apply_fixes",
                "finalize": "finalize_output"
            }
        )
        
        workflow.add_edge("apply_manual_mappings", "apply_fixes")
        workflow.add_edge("apply_fixes", "finalize_output")
        workflow.add_edge("finalize_output", END)
        
        return workflow.compile()
    
    def _map_columns_node(self, state: DataProcessingState) -> DataProcessingState:
        """Map source columns to canonical schema"""
        df = state["raw_dataframe"]
        source_columns = df.columns.tolist()
        
        # Get automatic mappings
        mappings = self.schema_mapper.map_columns(source_columns)
        
        # Get unmapped columns
        unmapped = self.schema_mapper.get_unmapped_columns(source_columns, mappings)
        
        state["column_mappings"] = mappings
        state["unmapped_columns"] = unmapped
        
        return state
    
    def _clean_data_node(self, state: DataProcessingState) -> DataProcessingState:
        """Clean the data using deterministic rules"""
        df = state["raw_dataframe"]
        mappings = state["column_mappings"]
        
        # Apply cleaning
        cleaned_df, cleaning_results = self.data_cleaner.clean_data(df, mappings)
        
        state["cleaned_dataframe"] = cleaned_df
        state["cleaning_results"] = cleaning_results
        
        return state
    
    def _validate_data_node(self, state: DataProcessingState) -> DataProcessingState:
        """Validate the cleaned data"""
        df = state["cleaned_dataframe"]
        
        # Validate data quality
        validation_errors = self.llm_fix_agent.validate_data_quality(df)
        
        state["validation_errors"] = validation_errors
        
        return state
    
    def _generate_fixes_node(self, state: DataProcessingState) -> DataProcessingState:
        """Generate fix suggestions for validation errors"""
        df = state["cleaned_dataframe"]
        validation_errors = state["validation_errors"]
        
        # Generate fix suggestions
        fix_suggestions = self.llm_fix_agent.generate_fix_suggestions(df, validation_errors)
        
        state["fix_suggestions"] = fix_suggestions
        
        return state
    
    def _should_apply_manual_mappings(self, state: DataProcessingState) -> str:
        """Determine if manual mappings are needed"""
        unmapped_columns = state.get("unmapped_columns", [])
        manual_mappings = state.get("manual_mappings", {})
        
        if unmapped_columns and not manual_mappings:
            return "manual_mapping"
        elif state.get("fix_suggestions"):
            return "apply_fixes"
        else:
            return "finalize"
    
    def _apply_manual_mappings_node(self, state: DataProcessingState) -> DataProcessingState:
        """Apply manual column mappings"""
        manual_mappings = state.get("manual_mappings", {})
        
        if manual_mappings:
            # Add manual mappings to the column mappings
            existing_mappings = state.get("column_mappings", [])
            
            for source_col, target_col in manual_mappings.items():
                manual_mapping = MappingResult(
                    source_column=source_col,
                    target_column=target_col,
                    confidence=1.0,
                    mapping_type='manual'
                )
                existing_mappings.append(manual_mapping)
            
            state["column_mappings"] = existing_mappings
            
            # Re-clean data with new mappings
            df = state["raw_dataframe"]
            cleaned_df, cleaning_results = self.data_cleaner.clean_data(df, existing_mappings)
            state["cleaned_dataframe"] = cleaned_df
            state["cleaning_results"] = cleaning_results
        
        return state
    
    def _apply_fixes_node(self, state: DataProcessingState) -> DataProcessingState:
        """Apply selected fix suggestions"""
        df = state["cleaned_dataframe"]
        applied_fixes = state.get("applied_fixes", [])
        
        if applied_fixes:
            # Apply the fixes to the dataframe
            for fix in applied_fixes:
                column = fix["column"]
                row_index = fix["row_index"]
                new_value = fix["suggested_value"]
                
                if column in df.columns:
                    df.loc[row_index, column] = new_value
            
            state["cleaned_dataframe"] = df
            
            # Promote fixes to learned fixes
            for fix in applied_fixes:
                fix_suggestion = FixSuggestion(
                    column=fix["column"],
                    row_index=fix["row_index"],
                    original_value=fix["original_value"],
                    suggested_value=fix["suggested_value"],
                    reason=fix["reason"],
                    confidence=fix["confidence"],
                    fix_type="applied"
                )
                self.llm_fix_agent.promote_fix(fix_suggestion)
        
        return state
    
    def _finalize_output_node(self, state: DataProcessingState) -> DataProcessingState:
        """Finalize the output and create summary"""
        df = state["cleaned_dataframe"]
        
        # Create processing summary
        summary = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "mapped_columns": len(state.get("column_mappings", [])),
            "unmapped_columns": len(state.get("unmapped_columns", [])),
            "cleaning_rules_applied": len(state.get("cleaning_results", [])),
            "validation_errors": len(state.get("validation_errors", [])),
            "fix_suggestions": len(state.get("fix_suggestions", [])),
            "applied_fixes": len(state.get("applied_fixes", []))
        }
        
        state["final_dataframe"] = df
        state["processing_summary"] = summary
        
        return state
    
    def process_data(self, df: pd.DataFrame, manual_mappings: Dict[str, str] = None, 
                    applied_fixes: List[Dict] = None) -> Dict[str, Any]:
        """Process the data through the complete pipeline"""
        initial_state = DataProcessingState(
            raw_dataframe=df,
            canonical_schema=self.canonical_schema,
            column_mappings=[],
            unmapped_columns=[],
            cleaned_dataframe=pd.DataFrame(),
            cleaning_results=[],
            validation_errors=[],
            fix_suggestions=[],
            manual_mappings=manual_mappings or {},
            applied_fixes=applied_fixes or [],
            final_dataframe=pd.DataFrame(),
            processing_summary={}
        )
        
        # Run the graph
        final_state = self.graph.invoke(initial_state)
        
        return {
            "final_dataframe": final_state["final_dataframe"],
            "column_mappings": final_state["column_mappings"],
            "unmapped_columns": final_state["unmapped_columns"],
            "cleaning_results": final_state["cleaning_results"],
            "validation_errors": final_state["validation_errors"],
            "fix_suggestions": final_state["fix_suggestions"],
            "processing_summary": final_state["processing_summary"]
        }