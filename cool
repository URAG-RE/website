// Name: WebSocket
// ID: gsaWebsocket
// Description: Manually connect to WebSocket servers.
// By: RedMan13 <https://scratch.mit.edu/users/RedMan13/>
// License: MIT

/* generated l10n code */
Scratch.translate.setup({
  "fi": { /* Finnish translations */ },
  "ja": { /* Japanese translations */ },
  "ko": { /* Korean translations */ },
  "ru": { /* Russian translations */ },
  "zh-cn": { /* Chinese (Simplified) translations */ }
});
/* end generated l10n code */

(function (Scratch) {
  "use strict";

  if (!Scratch.extensions.unsandboxed) {
    throw new Error("can not load outside unsandboxed mode");
  }

  const blobToDataURL = (blob) =>
    new Promise((resolve, reject) => {
      const fr = new FileReader();
      fr.onload = () => resolve(fr.result);
      fr.onerror = () =>
        reject(new Error(`Failed to read as data URL: ${fr.error}`));
      fr.readAsDataURL(blob);
    });

  const { BlockType, Cast, ArgumentType } = Scratch;
  const vm = Scratch.vm;
  const runtime = vm.runtime;

  const toCloseCode = (exitCode) => {
    const casted = Cast.toNumber(exitCode);
    if (casted === 1000 || (casted >= 3000 && casted <= 4999)) {
      return casted;
    }
    return 1000;
  };

  const toCloseReason = (reason) => {
    const casted = Cast.toString(reason);
    const encoder = new TextEncoder();
    let encoded = encoder.encode(casted).slice(0, 123);
    const decoder = new TextDecoder();
    while (encoded.byteLength > 0) {
      try {
        return decoder.decode(encoded);
      } catch {
        encoded = encoded.slice(0, encoded.byteLength - 1);
      }
    }
    return "";
  };

  class WebSocketExtension {
    constructor() {
      this.instances = {};
      runtime.on("targetWasRemoved", (target) => {
        const instance = this.instances[target.id];
        if (instance) {
          instance.destroyed = true;
          if (instance.websocket) instance.websocket.close();
          delete this.instances[target.id];
        }
      });
    }

    getInfo() {
      return {
        id: "gsaWebsocket",
        name: "WebSocket",
        docsURI: "https://extensions.turbowarp.org/godslayerakp/ws",
        color1: "#307eff",
        color2: "#2c5eb0",
        blocks: [
          { opcode: "newInstance", blockType: BlockType.COMMAND, arguments: { URL: { type: ArgumentType.STRING, defaultValue: "wss://echoserver.redman13.repl.co" } }, text: Scratch.translate("connect to [URL]") },
          "---",
          { opcode: "onOpen", blockType: BlockType.EVENT, isEdgeActivated: false, shouldRestartExistingThreads: true, text: Scratch.translate("when connected") },
          { opcode: "isConnected", blockType: BlockType.BOOLEAN, text: Scratch.translate("is connected?"), disableMonitor: true },
          "---",
          { opcode: "onMessage", blockType: BlockType.EVENT, isEdgeActivated: false, shouldRestartExistingThreads: true, text: Scratch.translate("when message received") },
          { opcode: "messageData", blockType: BlockType.REPORTER, text: Scratch.translate("received message data"), disableMonitor: true },
          "---",
          { opcode: "sendMessage", blockType: BlockType.COMMAND, arguments: { PAYLOAD: { type: ArgumentType.STRING, defaultValue: "Hello!" } }, text: Scratch.translate("send message [PAYLOAD]") },
          "---",
          { opcode: "onError", blockType: BlockType.EVENT, isEdgeActivated: false, shouldRestartExistingThreads: true, text: Scratch.translate("when connection errors") },
          { opcode: "hasErrored", blockType: BlockType.BOOLEAN, text: Scratch.translate("connection errored?"), disableMonitor: true },
          "---",
          { opcode: "onClose", blockType: BlockType.EVENT, isEdgeActivated: false, shouldRestartExistingThreads: true, text: Scratch.translate("when connection closes") },
          { opcode: "isClosed", blockType: BlockType.BOOLEAN, text: Scratch.translate("is connection closed?"), disableMonitor: true },
          { opcode: "closeCode", blockType: BlockType.REPORTER, text: Scratch.translate("closing code"), disableMonitor: true },
          { opcode: "closeMessage", blockType: BlockType.REPORTER, text: Scratch.translate("closing message"), disableMonitor: true },
          { opcode: "closeWithoutReason", blockType: BlockType.COMMAND, text: Scratch.translate("close connection") },
          { opcode: "closeWithCode", blockType: BlockType.COMMAND, arguments: { CODE: { type: ArgumentType.NUMBER, defaultValue: "1000" } }, text: Scratch.translate("close connection with code [CODE]") },
          { opcode: "closeWithReason", blockType: BlockType.COMMAND, arguments: { CODE: { type: ArgumentType.NUMBER, defaultValue: "1000" }, REASON: { type: ArgumentType.STRING, defaultValue: "fulfilled" } }, text: Scratch.translate("close connection with reason [REASON] and code [CODE]") }
        ]
      };
    }

    newInstance(args, util) {
      const target = util.target;
      let url = Cast.toString(args.URL);

      if (!/^(ws|wss):/is.test(url)) {
        if (/^(?!(ws|http)s?:\/\/).*$/is.test(url)) {
          url = `wss://${url}`;
        } else if (/^(http|https):/is.test(url)) {
          const parts = url.split(":");
          parts[0] = url.toLowerCase().startsWith("https") ? "wss" : "ws";
          url = parts.join(":");
        } else {
          return;
        }
      }

      const old = this.instances[target.id];
      if (old) {
        old.destroyed = true;
        if (old.websocket) old.websocket.close();
      }

      const instance = {
        destroyed: false,
        errored: false,
        closeMessage: "",
        closeCode: 0,
        data: "",
        websocket: null,
        messageThreadsRunning: false,
        connectThreads: [],
        messageThreads: [],
        messageQueue: [],
        sendOnceConnected: [],
      };
      this.instances[target.id] = instance;

      const canFetchPromise = Scratch.canFetch ? Scratch.canFetch(url) : Promise.resolve(true);

      return canFetchPromise.then((allowed) => new Promise((resolve) => {
        if (!allowed) throw new Error("Not allowed");
        if (instance.destroyed) return resolve();

        const ws = new WebSocket(url);
        instance.websocket = ws;

        const beforeExecute = () => {
          if (instance.messageThreadsRunning) {
            const stillRunning = instance.messageThreads.some((t) => runtime.isActiveThread(t));
            if (!stillRunning) {
              if (instance.messageQueue.length === 0) {
                instance.messageThreadsRunning = false;
                instance.messageThreads = [];
              } else {
                instance.data = instance.messageQueue.shift();
                instance.messageThreads = runtime.startHats("gsaWebsocket_onMessage", null, target);
              }
            }
          }
        };

        const onStopAll = () => {
          instance.destroyed = true;
          ws.close();
        };

        runtime.on("BEFORE_EXECUTE", beforeExecute);
        runtime.on("PROJECT_STOP_ALL", onStopAll);

        const cleanup = () => {
          runtime.off("BEFORE_EXECUTE", beforeExecute);
          runtime.off("PROJECT_STOP_ALL", onStopAll);
          for (const thread of instance.connectThreads) thread.status = 4;
          resolve();
        };

        ws.onopen = () => {
          if (instance.destroyed) {
            cleanup();
            ws.close();
            return;
          }
          for (const m of instance.sendOnceConnected) ws.send(m);
          instance.sendOnceConnected.length = 0;
          instance.connectThreads = runtime.startHats("gsaWebsocket_onOpen", null, target);
          resolve();
        };

        ws.onclose = (e) => {
          if (!instance.errored) {
            instance.closeMessage = e.reason || "";
            instance.closeCode = e.code;
            cleanup();
            if (!instance.destroyed) runtime.startHats("gsaWebsocket_onClose", null, target);
          }
        };

        ws.onerror = (e) => {
          console.error("websocket error", e);
          instance.errored = true;
          cleanup();
          if (!instance.destroyed) runtime.startHats("gsaWebsocket_onError", null, target);
        };

        ws.onmessage = async (e) => {
          if (instance.destroyed) return;
          let data = e.data;
          if (data instanceof Blob) data = await blobToDataURL(data);
          if (instance.messageThreadsRunning) {
            instance.messageQueue.push(data);
          } else {
            instance.data = data;
            instance.messageThreads = runtime.startHats("gsaWebsocket_onMessage", null, target);
            instance.messageThreadsRunning = true;
          }
        };
      })).catch((err) => {
        console.error("could not open websocket connection", err);
        instance.errored = true;
        if (!instance.destroyed) runtime.startHats("gsaWebsocket_onError", null, target);
      });
    }

    isConnected(_, utils) {
      const i = this.instances[utils.target.id];
      return i && i.websocket && i.websocket.readyState === WebSocket.OPEN;
    }

    messageData(_, utils) {
      const i = this.instances[utils.target.id];
      return i ? i.data : "";
    }

    isClosed(_, utils) {
      const i = this.instances[utils.target.id];
      return i ? i.closeCode !== 0 : false;
    }

    closeCode(_, utils) {
      const i = this.instances[utils.target.id];
      return i ? i.closeCode : 0;
    }

    closeMessage(_, utils) {
      const i = this.instances[utils.target.id];
      return i ? i.closeMessage : "";
    }

    hasErrored(_, utils) {
      const i = this.instances[utils.target.id];
      return i ? i.errored : false;
    }

    sendMessage(args, utils) {
      const i = this.instances[utils.target.id];
      if (!i) return;
      const msg = Cast.toString(args.PAYLOAD);
      if (!i.websocket || i.websocket.readyState === WebSocket.CONNECTING) {
        i.sendOnceConnected.push(msg);
      } else {
        i.websocket.send(msg);
      }
    }

    closeWithoutReason(_, utils) {
      const i = this.instances[utils.target.id];
      if (!i) return;
      i.destroyed = true;
      if (i.websocket) i.websocket.close();
    }

    closeWithCode(args, utils) {
      const i = this.instances[utils.target.id];
      if (!i) return;
      i.destroyed = true;
      if (i.websocket) i.websocket.close(toCloseCode(args.CODE));
    }

    closeWithReason(args, utils) {
      const i = this.instances[utils.target.id];
      if (!i) return;
      i.destroyed = true;
      if (i.websocket) {
        i.websocket.close(toCloseCode(args.CODE), toCloseReason(args.REASON));
      }
    }
  }

  Scratch.extensions.register(new WebSocketExtension());
})(Scratch);
