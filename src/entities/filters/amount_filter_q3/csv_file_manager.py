import csv
import os
import threading
from pathlib import Path
from typing import List, Tuple, Dict, Any


class CSVFileManager:
    """
    Manager for persisting pending transaction data to CSV files with thread-safe locks.
    Each client has its own CSV file with the client_id as filename.
    """

    # CSV column headers matching TransactionData fields
    # Internal IDs are stored to preserve dedup information while rows wait for averages.
    CSV_HEADERS = [
        "_data_id",
        "_message_id",
        "timestamp",
        "from_bank",
        "account_origin",
        "to_bank",
        "account_destination",
        "amount_received",
        "receiving_currency",
        "amount_paid",
        "payment_currency",
        "payment_format",
        "is_laundering"
    ]

    def __init__(self, data_pending_dir: str = "/data_pending"):
        """
        Initialize the CSV File Manager.
        
        Args:
            data_pending_dir: Directory where CSV files will be stored
        """
        self.data_pending_dir = data_pending_dir
        self._file_locks: Dict[str, threading.Lock] = {}
        self._locks_dict_lock = threading.Lock()
        
        # Create data_pending directory if it doesn't exist
        Path(self.data_pending_dir).mkdir(parents=True, exist_ok=True)

    def _get_file_lock(self, client_id: str) -> threading.Lock:
        """Get or create a lock for a specific client."""
        with self._locks_dict_lock:
            if client_id not in self._file_locks:
                self._file_locks[client_id] = threading.Lock()
            return self._file_locks[client_id]

    def _get_csv_path(self, client_id: str) -> str:
        """Get the CSV file path for a client."""
        return os.path.join(self.data_pending_dir, f"{client_id}.csv")

    def _file_exists(self, client_id: str) -> bool:
        """Check if CSV file exists for a client."""
        return os.path.exists(self._get_csv_path(client_id))

    def _initialize_csv_file(self, client_id: str) -> None:
        """Create CSV file with headers if it doesn't exist."""
        csv_path = self._get_csv_path(client_id)
        if not os.path.exists(csv_path):
            with open(csv_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
                writer.writeheader()

    def append_transaction(
        self,
        client_id: str,
        transaction_data: Dict[str, Any],
        data_id: str = "",
        message_id: str = "",
    ) -> None:
        """
        Append a transaction to the client's CSV file (thread-safe).
        
        Args:
            client_id: The client ID
            transaction_data: Dictionary containing transaction data
            data_id: The original data ID from the pipeline
            message_id: The original message ID used for deduplication
        """
        lock = self._get_file_lock(client_id)
        
        with lock:
            self._initialize_csv_file(client_id)
            csv_path = self._get_csv_path(client_id)
            
            # Extract only the fields that match CSV headers
            row = {header: transaction_data.get(header, "") for header in self.CSV_HEADERS}
            row["_data_id"] = data_id  # Include the data_id
            row["_message_id"] = message_id
            
            with open(csv_path, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.CSV_HEADERS)
                writer.writerow(row)

    def read_all_transactions(self, client_id: str) -> List[Tuple[Dict[str, Any], str, str]]:
        """
        Read all transactions from client's CSV file (thread-safe).
        Converts numeric fields back to their correct types.
        Returns list of tuples (transaction_data, data_id, message_id).
        
        Args:
            client_id: The client ID
            
        Returns:
            List of tuples containing (transaction_data, data_id, message_id)
        """
        lock = self._get_file_lock(client_id)
        transactions = []
        
        # Fields that should be converted to float
        float_fields = {"amount_received", "amount_paid"}
        
        with lock:
            csv_path = self._get_csv_path(client_id)
            if not os.path.exists(csv_path):
                return transactions
            
            with open(csv_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Extract internal IDs and remove them from the transaction data.
                    data_id = row.pop("_data_id", "")
                    message_id = row.pop("_message_id", "")
                    transaction_data = dict(row)
                    
                    # Convert float fields to their correct type
                    for field in float_fields:
                        if field in transaction_data and transaction_data[field]:
                            try:
                                transaction_data[field] = float(transaction_data[field])
                            except (ValueError, TypeError):
                                # If conversion fails, keep as string or set to 0
                                transaction_data[field] = 0.0
                    
                    transactions.append((transaction_data, data_id, message_id))
        
        return transactions

    def delete_csv_file(self, client_id: str) -> None:
        """
        Delete the CSV file for a client after processing is complete (thread-safe).
        
        Args:
            client_id: The client ID
        """
        lock = self._get_file_lock(client_id)
        
        with lock:
            csv_path = self._get_csv_path(client_id)
            if os.path.exists(csv_path):
                os.remove(csv_path)

    def clear_all_files(self) -> None:
        """Delete all CSV files in data_pending directory."""
        for file in os.listdir(self.data_pending_dir):
            if file.endswith(".csv"):
                os.remove(os.path.join(self.data_pending_dir, file))
