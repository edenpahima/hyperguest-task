import {  Message } from "./Database";

export class Queue {
    private mapKeyToMessagesOnKey: {[key: string]: Message[] }
    private freeKeys: string[]
    private size: number;
    private mapWorkerIdToKey: {[key: number]: string }

    constructor() {
        this.mapKeyToMessagesOnKey = {}
        this.mapWorkerIdToKey = {}
        this.freeKeys = []
        this.size = 0;
    }

    Enqueue = (message: Message) => {
        if(!this.mapKeyToMessagesOnKey[message.key]){
            this.mapKeyToMessagesOnKey[message.key] = []
            this.freeKeys.push(message.key);
        }
        this.mapKeyToMessagesOnKey[message.key].push(message)
        this.size++;
    }

    Dequeue = (workerId: number): Message | undefined => {
        if(this.freeKeys.length > 0){
            const key = this.freeKeys.splice(0,1)[0];
            const message = this.mapKeyToMessagesOnKey[key].splice(0,1)[0];
            this.mapWorkerIdToKey[workerId] = key;
            this.size--;
            return message;
        }
        return undefined;
    }

    Confirm = (workerId: number, messageId: string) => {
        const key = this.mapWorkerIdToKey[workerId];
        if(this.mapKeyToMessagesOnKey[key].length > 0){
            this.freeKeys.push(key)
        }
        else{
            delete this.mapKeyToMessagesOnKey[key]
            delete this.mapWorkerIdToKey[workerId]
        }
    }

    Size = () => {
        return this.size;
    }
}

