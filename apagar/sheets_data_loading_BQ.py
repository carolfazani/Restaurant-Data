from google.auth import default

def verificar_conta_logada():
    # Obtém as credenciais padrão
    credentials, project_id = default()

    # Exibe o e-mail da conta autenticada, se aplicável
    if credentials is None:
        print("Nenhuma credencial padrão encontrada.")
    elif credentials.valid:
        if hasattr(credentials, 'service_account_email'):
            print("Conta autenticada:", credentials.service_account_email)
        else:
            print("Conta autenticada:", credentials.client_id)
    else:
        print("Credenciais padrão inválidas ou expiradas.")

if __name__ == "__main__":
    verificar_conta_logada()
