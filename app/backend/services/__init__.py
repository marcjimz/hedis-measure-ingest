"""
Services Package

Contains business logic and state management services.

Note: Imports are lazy to avoid loading pyspark in mock mode.
Use direct imports from submodules instead:
    from services.chat_history_manager import ChatHistoryManager
    from services.delta_table_chat_history import DeltaTableChatHistoryManager
"""

__all__ = [
    "ChatHistoryManager",
    "DeltaTableChatHistoryManager"
]

def __getattr__(name):
    """Lazy import to avoid loading pyspark in mock mode."""
    if name == "ChatHistoryManager":
        from services.chat_history_manager import ChatHistoryManager
        return ChatHistoryManager
    elif name == "DeltaTableChatHistoryManager":
        from services.delta_table_chat_history import DeltaTableChatHistoryManager
        return DeltaTableChatHistoryManager
    raise AttributeError(f"module 'services' has no attribute '{name}'")
