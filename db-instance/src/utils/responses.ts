export class GRPCResponses {
  static success(data: any) {
    return {
      success: true,
      resultJson: JSON.stringify(data),
    };
  }

  static error(message: string) {
    return {
      success: false,
      message,
    };
  }
}