declare module 'claude-code' {
  interface PluginState {
    tg: { chats: string[] }
  }
}
export type Chats = string[]
