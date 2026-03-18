/**
 * Módulo WhatsApp — índice.
 * Uso: formatação do texto (formatAlteracaoMessage) e API (whatsappApi).
 */

export { formatAlteracaoMessage } from "./formatAlteracaoMessage";
export type { WhatsAppFormPayload } from "./formatAlteracaoMessage";
export { getConnectionStatus, getQrImage, getQrImageUrl, getQrImageUrlWithTimestamp, getGroups, sendToGroup, connectWhatsApp, disconnectWhatsApp } from "./whatsappApi";
export type { WhatsAppGroup, ConnectionStatus, WhatsAppAttachment } from "./whatsappApi";
