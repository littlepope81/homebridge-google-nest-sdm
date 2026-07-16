"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MotionAccessory = void 0;
const Accessory_1 = require("./Accessory");
class MotionAccessory extends Accessory_1.Accessory {
    constructor(api, log, platform, accessory, device) {
        super(api, log, platform, accessory, device);
        this.motionDecay = 20000;
        //create a new Motion service
        this.motionService = accessory.getService(this.api.hap.Service.MotionSensor);
        if (!this.motionService) {
            this.motionService = accessory.addService(this.api.hap.Service.MotionSensor);
        }
        this.motionService.getCharacteristic(this.platform.Characteristic.MotionDetected)
            .onGet(this.handleMotionDetectedGet.bind(this));
        this.device.onMotion = this.handleMotion.bind(this);
    }
    handleMotion() {
        this.log.debug('Motion detected!', this.accessory.displayName);
        this.lastMotion = Date.now();
        this.motionService.updateCharacteristic(this.platform.Characteristic.MotionDetected, true);
        this.scheduleMotionDecay(this.motionDecay);
    }
    // This sensor is the camera controller's HKSV motion trigger, and the
    // recording generator ends recordings only when MotionDetected goes false —
    // a sensor stuck "true" means unbounded recording. So the decay must be
    // airtight: >= (a timer firing at exactly the decay boundary must clear),
    // and if this timer raced a newer motion event, re-arm for the remainder
    // instead of relying on the newer event's own timer.
    scheduleMotionDecay(delay) {
        if (this.motionDecayTimer)
            clearTimeout(this.motionDecayTimer);
        this.motionDecayTimer = setTimeout(() => {
            this.motionDecayTimer = undefined;
            if (!this.lastMotion || Date.now() - this.lastMotion >= this.motionDecay) {
                this.lastMotion = undefined;
                this.motionService.updateCharacteristic(this.platform.Characteristic.MotionDetected, false);
            }
            else {
                this.scheduleMotionDecay(this.motionDecay - (Date.now() - this.lastMotion));
            }
        }, delay);
    }
    handleMotionDetectedGet() {
        return !!(this.lastMotion && Date.now() - this.lastMotion <= this.motionDecay);
    }
}
exports.MotionAccessory = MotionAccessory;
//# sourceMappingURL=MotionAccessory.js.map