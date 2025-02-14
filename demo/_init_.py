import logging
import json
from azure.data.tables import TableServiceClient, TableEntity
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Procesando una solicitud de reserva.")

    # Obtener parámetros del cuerpo de la solicitud
    try:
        req_body = req.get_json()
        usuario_id = req_body.get("usuarioId")
        fecha = req_body.get("fecha")
        hora = req_body.get("hora")
    except ValueError:
        return func.HttpResponse(
            "Solicitud no válida. Se requiere un cuerpo JSON.",
            status_code=400
        )

    if not usuario_id or not fecha or not hora:
        return func.HttpResponse(
            "Faltan datos obligatorios: usuarioId, fecha y hora.",
            status_code=400
        )

    # Conectar con Azure Table Storage
    connection_string = "<CONNECTION_STRING_DE_STORAGE>"
    table_name = "Reservas"

    try:
        table_service = TableServiceClient.from_connection_string(conn_str=connection_string)
        table_client = table_service.get_table_client(table_name)

        # Validar si ya existe una reserva para ese horario
        fecha_hora = f"{fecha}T{hora}"
        query_filter = f"PartitionKey eq '{fecha}' and RowKey eq '{hora}'"
        existing_reservation = list(table_client.query_entities(query_filter))

        if existing_reservation:
            return func.HttpResponse(
                "El horario ya está reservado.",
                status_code=409
            )

        # Crear una nueva reserva
        reserva = {
            "PartitionKey": fecha,
            "RowKey": hora,
            "UsuarioID": usuario_id,
            "Timestamp": str(datetime.utcnow())
        }
        table_client.create_entity(entity=reserva)

        return func.HttpResponse(
            json.dumps({"message": "Reserva creada con éxito."}),
            status_code=201,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error procesando la solicitud: {e}")
        return func.HttpResponse(
            "Error interno del servidor.",
            status_code=500
        )
