import os 
from dotenv import load_dotenv
import hvac

load_dotenv()

VAULT_URL = os.getenv("VAULT_PATH")

USERNAME = os.getenv("VAULT_USER")
PASSWORD = os.getenv("VAULT_PASSWORD")

def get_vault_token_userpass(username, password):
    client = hvac.Client(url=VAULT_URL)
    login_response = client.auth.userpass.login(
        username=username,
        password=password,
    )
    
    if client.is_authenticated():
        print("Successfully authenticated with Vault.")
        return login_response['auth']['client_token']
    else:
        print("Failed to authenticate with Vault.")
        return None

def get_secret_from_vault(token, secret_path, mount_point='kv'):
    try:
        client = hvac.Client(url=VAULT_URL, token=token)
        
        # Read the secret from the specified path with mount point
        response = client.secrets.kv.v2.read_secret_version(
            path='gold',
            mount_point=mount_point
        )
        
        if response and 'data' in response:
            secret_data = response['data']['data']
            print(f"Successfully retrieved secret from {secret_path}")
            return secret_data
        else:
            print(f"No secret found at path: {secret_path}")
            return None
            
    except Exception as e:
        print(f"Error retrieving secret: {str(e)}")
        return None

if __name__ == "__main__":
    if USERNAME and PASSWORD:
        token = get_vault_token_userpass(USERNAME, PASSWORD)
        if token:
            print(f"Vault Token: {token}")
            
            # Retrieve the secret from the specified path with mount point
            secret_path = "/v1/kv/data/gold"
            mount_point = "kv"  # Specify your mount point here
            secret_data = get_secret_from_vault(token, secret_path, mount_point)
            
            if secret_data:
                print("Secret data retrieved:")
                for key, value in secret_data.items():
                    print(f"  {key}: {value}")
            else:
                print("Failed to retrieve secret data.")
    else:
        print("Username or password not set in environment variables.")