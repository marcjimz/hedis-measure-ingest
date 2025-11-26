"""
Services Package

Contains business logic and state management services.

Note: Imports are lazy to avoid loading heavy dependencies in mock mode.
Use direct imports from submodules instead:
    from services.chat_history_manager import ChatHistoryManager
    from services.sql_warehouse_chat_history import SQLWarehouseChatHistoryManager
"""

__all__ = [
    "ChatHistoryManager",
    "SQLWarehouseChatHistoryManager"
]

def __getattr__(name):
    """Lazy import to avoid loading heavy dependencies in mock mode."""
    if name == "ChatHistoryManager":
        from services.chat_history_manager import ChatHistoryManager
        return ChatHistoryManager
    elif name == "SQLWarehouseChatHistoryManager":
        from services.sql_warehouse_chat_history import SQLWarehouseChatHistoryManager
        return SQLWarehouseChatHistoryManager
    raise AttributeError(f"module 'services' has no attribute '{name}'")
