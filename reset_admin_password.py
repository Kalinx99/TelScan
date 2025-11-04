
import json
from sqlalchemy import create_engine, text
from werkzeug.security import generate_password_hash
import getpass
import os

def reset_admin_password():
    """
    Resets the admin password for the TelScan application.
    """
    try:
        # Load database configuration
        config_path = os.path.join(os.path.dirname(__file__), 'mysql.json')
        with open(config_path, 'r') as f:
            db_config = json.load(f)

        # Create database engine
        db_uri = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            "?charset=utf8mb4"
        )
        engine = create_engine(db_uri)

        # Get new password
        new_password = getpass.getpass("Enter new password for admin: ")
        confirm_password = getpass.getpass("Confirm new password: ")

        if new_password != confirm_password:
            print("Passwords do not match. Aborting.")
            return

        if not new_password:
            print("Password cannot be empty. Aborting.")
            return

        # Hash the new password
        hashed_password = generate_password_hash(new_password)

        # Update the database
        with engine.connect() as connection:
            update_query = text("UPDATE user SET password_hash = :password_hash WHERE username = 'admin'")
            result = connection.execute(update_query, {"password_hash": hashed_password})
            connection.commit()

            if result.rowcount > 0:
                print("Admin password has been reset successfully.")
            else:
                print("Admin user not found. No changes were made.")

    except FileNotFoundError:
        print(f"Error: Database configuration file 'mysql.json' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    reset_admin_password()
