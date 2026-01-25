import axios from 'axios';

export interface DoubanComment {
    title: string;
    type: string;
    myComment_comment: string;
    myComment_MyValue: number;
    myComment_create_time: string;
}

const apiClient = axios.create({
    baseURL: 'http://127.0.0.1:5000/api',
    timeout: 10000,
});

export const fetchDoubanComments = async (): Promise<DoubanComment[]> => {
    try {
        const response = await apiClient.get<DoubanComment[]>('/douban');
        // 后端可能直接返回数组，也可能包裹在 data 字段中，根据用户描述“后端会返回一个arr”，假设直接是 data
        // 也就是 response.data 就是那个数组
        return response.data;
    } catch (error) {
        console.error('Error fetching douban comments:', error);
        throw error;
    }
};
